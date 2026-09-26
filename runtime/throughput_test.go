package runtime

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"
)

// /debug/throughput reports zero rates on the first sample and real rates
// once a second sample can be diffed against the first.
func TestDebugThroughputEndpoint(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	events := crashTestEvents(base, 50)
	source := &ScriptedSource{Events: events, Interval: 20 * time.Millisecond}
	sink := &collectSink{}
	g := NewGraph(source,
		func(e Event) Event { return e },
		func(e Event) string { return e.Key },
		sink)
	g.SetParallelism(2)
	g.SetWatermarkBound(2 * time.Second)
	g.SetWindow(Tumbling(10*time.Second), Count())

	server := &HTTPServer{Graph: g}
	handler := server.Handler()

	runDone := make(chan struct{})
	go func() {
		_ = g.Run(context.Background())
		close(runDone)
	}()

	sample := func() throughputJSON {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest("GET", "/debug/throughput", nil))
		var out throughputJSON
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("decode throughput response: %v", err)
		}
		return out
	}

	first := sample()
	if first.SourcePerSec != 0 {
		t.Fatalf("first sample should report zero rate, got %v", first.SourcePerSec)
	}

	time.Sleep(300 * time.Millisecond)
	second := sample()
	if second.SourceRecords == 0 {
		t.Fatal("expected source records after 300ms")
	}
	if second.SourcePerSec <= 0 {
		t.Fatalf("expected positive source rate, got %v", second.SourcePerSec)
	}
	if len(second.Subtasks) != 2 {
		t.Fatalf("expected 2 subtasks, got %d", len(second.Subtasks))
	}

	<-runDone

	// After a full bounded run, totals must account for every event.
	snap := g.Throughput()
	if snap.SourceRecords != int64(len(events)) {
		t.Fatalf("source records = %d, want %d", snap.SourceRecords, len(events))
	}
	var processed int64
	for _, sub := range snap.Subtasks {
		processed += sub.Processed
		if sub.Watermark.IsZero() {
			t.Fatalf("subtask %d has no watermark after full run", sub.Index)
		}
	}
	if processed != int64(len(events)) {
		t.Fatalf("subtasks processed %d records, want %d", processed, len(events))
	}
}
