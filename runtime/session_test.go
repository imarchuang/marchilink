package runtime

import (
	"context"
	"strings"
	"testing"
	"time"
)

// Session windows grow as events arrive within the gap and close once the
// watermark passes last-event + gap.
func TestSessionWindowsMergeAndClose(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	source := &ScriptedSource{Events: []Event{
		windowTestEvent(base, "a", 0),  // [00,05)
		windowTestEvent(base, "a", 3),  // [03,08) touches [00,05) -> session [00,08) count 2
		windowTestEvent(base, "a", 20), // wm=18 -> fires [00,08)="2"; opens [20,25)
		// source ends -> final flush fires [20,25)
	}}

	sink := &recordingSink{}
	graph := NewGraph(source,
		func(event Event) Event { return event },
		func(event Event) string { return event.Key },
		sink)
	graph.SetParallelism(1)
	graph.SetWatermarkBound(2 * time.Second)
	graph.SetWindow(Session(5*time.Second), Count())

	if err := graph.Run(context.Background()); err != nil {
		t.Fatalf("run graph: %v", err)
	}

	got := resultsByWindowEnd(sink)
	if len(got) != 2 {
		t.Fatalf("expected 2 sessions, got %d: %v", len(got), got)
	}
	if v := got[base.Add(8*time.Second)]; len(v) != 1 || !strings.HasSuffix(v[0], " 2") {
		t.Fatalf("session [00,08) expected count 2, got %v", v)
	}
	if v := got[base.Add(25*time.Second)]; len(v) != 1 || !strings.HasSuffix(v[0], " 1") {
		t.Fatalf("session [20,25) expected count 1, got %v", v)
	}
	if snapshot := graph.Watermarks(); snapshot.PendingWindows != 0 {
		t.Fatalf("expected 0 pending windows after flush, got %d", snapshot.PendingWindows)
	}
}

// An out-of-order event that lands between two live sessions bridges them
// into one. The bridge must arrive while both sessions are still open,
// i.e. before the watermark passes either end.
func TestSessionWindowsBridgeOutOfOrder(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	source := &ScriptedSource{Events: []Event{
		windowTestEvent(base, "a", 0),  // [00,05)
		windowTestEvent(base, "a", 4),  // -> session [00,09) count 2 (wm=2)
		windowTestEvent(base, "a", 10), // wm=8, [00,09) still open; opens [10,15)
		windowTestEvent(base, "a", 7),  // [07,12) touches both -> bridge [00,15) count 4
		windowTestEvent(base, "a", 30), // wm=28 -> fires [00,15)="4"; opens [30,35)
		// final flush fires [30,35)
	}}

	sink := &recordingSink{}
	graph := NewGraph(source,
		func(event Event) Event { return event },
		func(event Event) string { return event.Key },
		sink)
	graph.SetParallelism(1)
	graph.SetWatermarkBound(2 * time.Second)
	graph.SetWindow(Session(5*time.Second), Count())

	if err := graph.Run(context.Background()); err != nil {
		t.Fatalf("run graph: %v", err)
	}

	got := resultsByWindowEnd(sink)
	if len(got) != 2 {
		t.Fatalf("expected 2 sessions, got %d: %v", len(got), got)
	}
	if v := got[base.Add(15*time.Second)]; len(v) != 1 || !strings.HasSuffix(v[0], " 4") {
		t.Fatalf("bridged session [00,15) expected count 4, got %v", v)
	}
	if v := got[base.Add(35*time.Second)]; len(v) != 1 || !strings.HasSuffix(v[0], " 1") {
		t.Fatalf("session [30,35) expected count 1, got %v", v)
	}
}

func TestSessionAssigner(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 9, 26, 12, 0, 7, 0, time.UTC)
	ws := Session(5 * time.Second).Assign(ts)
	if len(ws) != 1 {
		t.Fatalf("expected 1 window, got %v", ws)
	}
	if !ws[0].Start.Equal(ts) || !ws[0].End.Equal(ts.Add(5*time.Second)) {
		t.Fatalf("expected [ts, ts+5s), got %v", ws[0])
	}
}
