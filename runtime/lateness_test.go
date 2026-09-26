package runtime

import (
	"context"
	"strings"
	"testing"
	"time"
)

// With allowed lateness, a late record inside the grace period re-fires its
// window with an updated result; a record past the grace period goes to the
// side output instead of being silently dropped.
func TestAllowedLatenessRefiresAndSideOutputs(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	source := &ScriptedSource{Events: []Event{
		windowTestEvent(base, "a", 1),  // [00,10) count 1
		windowTestEvent(base, "a", 12), // wm=10 -> fires [00,10)="1"; lands in [10,20)
		windowTestEvent(base, "a", 5),  // late but within lateness (wm=10 < 15): re-fire [00,10)="2"
		windowTestEvent(base, "a", 20), // wm=18 -> purges [00,10) (15 <= 18); lands in [20,30)
		windowTestEvent(base, "a", 7),  // too late (wm=18 >= 15): side output
		windowTestEvent(base, "a", 30), // wm=28; lands in [30,40)
		// source ends -> final flush fires [10,20), [20,30), [30,40)
	}}

	sink := &recordingSink{}
	side := &recordingSink{}
	graph := NewGraph(source,
		func(event Event) Event { return event },
		func(event Event) string { return event.Key },
		sink)
	graph.SetParallelism(1)
	graph.SetWatermarkBound(2 * time.Second)
	graph.SetWindow(Tumbling(10*time.Second), Count())
	graph.SetAllowedLateness(5 * time.Second)
	graph.SetSideOutput(side)

	if err := graph.Run(context.Background()); err != nil {
		t.Fatalf("run graph: %v", err)
	}

	got := resultsByWindowEnd(sink)

	// [00,10) fired twice: once on time with count 1, once re-fired by the
	// late record with count 2.
	firstFire := got[base.Add(10*time.Second)]
	if len(firstFire) != 2 {
		t.Fatalf("window [00,10) expected 2 firings, got %d: %v", len(firstFire), firstFire)
	}
	if !strings.HasSuffix(firstFire[0], " 1") || !strings.HasSuffix(firstFire[1], " 2") {
		t.Fatalf("window [00,10) expected firings 1 then 2, got %v", firstFire)
	}

	// The other windows fired exactly once each.
	for _, end := range []time.Duration{20, 30, 40} {
		values := got[base.Add(end*time.Second)]
		if len(values) != 1 || !strings.HasSuffix(values[0], " 1") {
			t.Fatalf("window ending +%ds expected one firing of count 1, got %v", end, values)
		}
	}

	// The too-late record went to the side output, not the main sink.
	if len(side.events) != 1 {
		t.Fatalf("expected 1 side-output record, got %d", len(side.events))
	}
	if !side.events[0].event.Timestamp.Equal(base.Add(7 * time.Second)) {
		t.Fatalf("side-output record has ts %v, want %v", side.events[0].event.Timestamp, base.Add(7*time.Second))
	}

	snapshot := graph.Watermarks()
	if snapshot.LateRecords != 1 {
		t.Fatalf("expected 1 late record, got %d", snapshot.LateRecords)
	}
	if snapshot.PendingWindows != 0 {
		t.Fatalf("expected 0 pending windows after flush, got %d", snapshot.PendingWindows)
	}
}

// With zero allowed lateness (the default), a side output still captures
// late records — they are just never accepted into their window.
func TestSideOutputWithoutLateness(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	source := &ScriptedSource{Events: []Event{
		windowTestEvent(base, "a", 1),
		windowTestEvent(base, "a", 12), // wm=10 -> fires [00,10)="1" and purges it
		windowTestEvent(base, "a", 5),  // late: side output, no re-fire
	}}

	sink := &recordingSink{}
	side := &recordingSink{}
	graph := NewGraph(source,
		func(event Event) Event { return event },
		func(event Event) string { return event.Key },
		sink)
	graph.SetParallelism(1)
	graph.SetWatermarkBound(2 * time.Second)
	graph.SetWindow(Tumbling(10*time.Second), Count())
	graph.SetSideOutput(side)

	if err := graph.Run(context.Background()); err != nil {
		t.Fatalf("run graph: %v", err)
	}

	got := resultsByWindowEnd(sink)
	if values := got[base.Add(10*time.Second)]; len(values) != 1 || !strings.HasSuffix(values[0], " 1") {
		t.Fatalf("window [00,10) expected one firing of count 1, got %v", values)
	}
	if len(side.events) != 1 {
		t.Fatalf("expected 1 side-output record, got %d", len(side.events))
	}
	if snapshot := graph.Watermarks(); snapshot.LateRecords != 1 {
		t.Fatalf("expected 1 late record, got %d", snapshot.LateRecords)
	}
}
