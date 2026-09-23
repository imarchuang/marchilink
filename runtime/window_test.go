package runtime

import (
	"context"
	"strings"
	"testing"
	"time"
)

func windowTestEvent(base time.Time, key string, sec int) Event {
	return Event{Key: key, Value: "x", Timestamp: base.Add(time.Duration(sec) * time.Second)}
}

// resultsByWindowEnd maps window end timestamp -> emitted value.
func resultsByWindowEnd(sink *recordingSink) map[time.Time][]string {
	out := make(map[time.Time][]string)
	for _, r := range sink.events {
		out[r.event.Timestamp] = append(out[r.event.Timestamp], r.event.Value)
	}
	return out
}

func TestTumblingWindowOutOfOrderInputExactResults(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	source := ScriptedSource{Events: []Event{
		windowTestEvent(base, "a", 1),
		windowTestEvent(base, "a", 3),
		windowTestEvent(base, "a", 12), // wm=10 -> fires [00,10) with count 2
		windowTestEvent(base, "a", 13), // wm=11
		windowTestEvent(base, "a", 5),  // late: [00,10) already fired
		windowTestEvent(base, "a", 20), // wm=18; lands in [20,30), stays open
		// source ends -> final flush fires [10,20) and [20,30)
	}}

	sink := &recordingSink{}
	graph := NewGraph(source,
		func(event Event) Event { return event },
		func(event Event) string { return event.Key },
		sink)
	graph.SetParallelism(1)
	graph.SetWatermarkBound(2 * time.Second)
	graph.SetWindow(Tumbling(10*time.Second), Count())

	if err := graph.Run(context.Background()); err != nil {
		t.Fatalf("run graph: %v", err)
	}

	got := resultsByWindowEnd(sink)

	// Exactly three windows fired, each exactly once.
	if len(got) != 3 {
		t.Fatalf("expected 3 fired windows, got %d: %v", len(got), got)
	}
	for end, values := range got {
		if len(values) != 1 {
			t.Fatalf("window ending %s fired %d times: %v", end, len(values), values)
		}
	}

	if v := got[base.Add(10*time.Second)][0]; !strings.HasSuffix(v, " 2") {
		t.Fatalf("window [00,10) expected count 2, got %q", v)
	}
	if v := got[base.Add(20*time.Second)][0]; !strings.HasSuffix(v, " 2") {
		t.Fatalf("window [10,20) expected count 2, got %q", v)
	}
	if v := got[base.Add(30*time.Second)][0]; !strings.HasSuffix(v, " 1") {
		t.Fatalf("window [20,30) expected count 1, got %q", v)
	}

	snapshot := graph.Watermarks()
	if snapshot.LateRecords != 1 {
		t.Fatalf("expected 1 late record, got %d", snapshot.LateRecords)
	}
	if snapshot.PendingWindows != 0 {
		t.Fatalf("expected 0 pending windows after flush, got %d", snapshot.PendingWindows)
	}
}

func TestSlidingWindowAssignsOverlappingWindows(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	assigner := Sliding(10*time.Second, 5*time.Second)

	windows := assigner.Assign(base.Add(7 * time.Second))
	if len(windows) != 2 {
		t.Fatalf("expected 2 windows for ts=7s, got %d: %v", len(windows), windows)
	}
	if !windows[0].Start.Equal(base) || !windows[0].End.Equal(base.Add(10*time.Second)) {
		t.Fatalf("unexpected first window: %v", windows[0])
	}
	if !windows[1].Start.Equal(base.Add(5*time.Second)) || !windows[1].End.Equal(base.Add(15*time.Second)) {
		t.Fatalf("unexpected second window: %v", windows[1])
	}
}

func TestSlidingWindowEndToEnd(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	source := ScriptedSource{Events: []Event{
		windowTestEvent(base, "a", 7), // belongs to [00,10) and [05,15)
	}}

	sink := &recordingSink{}
	graph := NewGraph(source,
		func(event Event) Event { return event },
		func(event Event) string { return event.Key },
		sink)
	graph.SetParallelism(1)
	graph.SetWatermarkBound(time.Second)
	graph.SetWindow(Sliding(10*time.Second, 5*time.Second), Count())

	if err := graph.Run(context.Background()); err != nil {
		t.Fatalf("run graph: %v", err)
	}

	got := resultsByWindowEnd(sink)
	if len(got) != 2 {
		t.Fatalf("expected the event in 2 sliding windows, got %d: %v", len(got), got)
	}
	for end, values := range got {
		if !strings.HasSuffix(values[0], " 1") {
			t.Fatalf("window ending %s expected count 1, got %q", end, values[0])
		}
	}
}

func TestWindowsAreIsolatedPerKey(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	source := ScriptedSource{Events: []Event{
		windowTestEvent(base, "a", 1),
		windowTestEvent(base, "b", 2),
		windowTestEvent(base, "a", 3),
	}}

	sink := &recordingSink{}
	graph := NewGraph(source,
		func(event Event) Event { return event },
		func(event Event) string { return event.Key },
		sink)
	graph.SetParallelism(1)
	graph.SetWatermarkBound(time.Second)
	graph.SetWindow(Tumbling(10*time.Second), Count())

	if err := graph.Run(context.Background()); err != nil {
		t.Fatalf("run graph: %v", err)
	}

	got := make(map[string]string)
	for _, r := range sink.events {
		got[r.event.Key] = r.event.Value
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 keyed results, got %d: %v", len(got), got)
	}
	if !strings.HasSuffix(got["a"], " 2") {
		t.Fatalf("key a expected count 2, got %q", got["a"])
	}
	if !strings.HasSuffix(got["b"], " 1") {
		t.Fatalf("key b expected count 1, got %q", got["b"])
	}
}

func TestWindowFiresOnlyWhenWatermarkPassesEnd(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)

	// ts=9 lands in [00,10); wm becomes 7 -> window must NOT fire yet.
	// ts=11 lands in [10,20); wm becomes 9 -> still not past end=10? 9 < 10, no.
	// ts=12 lands in [10,20); wm becomes 10 -> fires [00,10).
	source := ScriptedSource{Events: []Event{
		windowTestEvent(base, "a", 9),
		windowTestEvent(base, "a", 11),
		windowTestEvent(base, "a", 12),
	}}

	sink := &recordingSink{}
	graph := NewGraph(source,
		func(event Event) Event { return event },
		func(event Event) string { return event.Key },
		sink)
	graph.SetParallelism(1)
	graph.SetWatermarkBound(2 * time.Second)
	graph.SetWindow(Tumbling(10*time.Second), Count())

	if err := graph.Run(context.Background()); err != nil {
		t.Fatalf("run graph: %v", err)
	}

	got := resultsByWindowEnd(sink)
	if v := got[base.Add(10*time.Second)][0]; !strings.HasSuffix(v, " 1") {
		t.Fatalf("window [00,10) expected count 1, got %q", v)
	}
	if v := got[base.Add(20*time.Second)][0]; !strings.HasSuffix(v, " 2") {
		t.Fatalf("window [10,20) expected count 2, got %q", v)
	}
}
