package runtime

import (
	"context"
	"testing"
	"time"
)

func TestWatermarksAdvanceMonotonicallyWithBoundedOutOfOrderness(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	source := ScriptedSource{Events: []Event{
		{Key: "a", Value: "1", Timestamp: base.Add(1 * time.Second)},
		{Key: "a", Value: "2", Timestamp: base.Add(4 * time.Second)},
		{Key: "a", Value: "3", Timestamp: base.Add(3 * time.Second)},
		{Key: "a", Value: "4", Timestamp: base.Add(8 * time.Second)},
	}}

	sink := &recordingSink{}
	graph := NewGraph(source, func(event Event) Event { return event }, func(event Event) string { return event.Key }, sink)
	graph.SetParallelism(2)
	graph.SetWatermarkBound(2 * time.Second)

	if err := graph.Run(context.Background()); err != nil {
		t.Fatalf("run graph: %v", err)
	}

	snapshot := graph.Watermarks()
	want := base.Add(8 * time.Second).Add(-2 * time.Second)
	if !snapshot.Source.Equal(want) {
		t.Fatalf("expected source watermark %s, got %s", want, snapshot.Source)
	}
	if snapshot.Min.After(snapshot.Source) {
		t.Fatalf("operator watermark %s should not overtake source watermark %s", snapshot.Min, snapshot.Source)
	}
}

func TestWatermarkTrackerTakesMinAcrossInputs(t *testing.T) {
	t.Parallel()

	tracker := NewWatermarkTracker(2, 0)
	base := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)

	if got := tracker.UpdateInput(0, base.Add(10*time.Second)); !got.Equal(base.Add(10 * time.Second)) {
		t.Fatalf("unexpected initial watermark: %s", got)
	}
	if got := tracker.UpdateInput(1, base.Add(6*time.Second)); !got.Equal(base.Add(10 * time.Second)) {
		t.Fatalf("expected watermark to remain monotonic, got %s", got)
	}
	if got := tracker.UpdateInput(1, base.Add(12*time.Second)); !got.Equal(base.Add(10 * time.Second)) {
		t.Fatalf("expected watermark to stay at min input, got %s", got)
	}
	if got := tracker.UpdateInput(0, base.Add(13*time.Second)); !got.Equal(base.Add(12 * time.Second)) {
		t.Fatalf("expected watermark to advance to min across inputs, got %s", got)
	}
}
