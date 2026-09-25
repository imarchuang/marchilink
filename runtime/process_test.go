package runtime

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

type countingSink struct {
	n atomic.Int64
}

func (s *countingSink) Write(context.Context, Event) error {
	s.n.Add(1)
	return nil
}

// runningCountProcess keeps a per-key counter in ValueState.
func runningCountProcess(ctx *StateContext, event Event) ([]Event, error) {
	count := ValueOf[int64](ctx, "running-count")
	n, _ := count.Get()
	n++
	count.Set(n)
	return []Event{event}, nil
}

func TestRunningCountAcrossMillionKeys(t *testing.T) {
	t.Parallel()

	const totalKeys = 1_000_000
	const repeats = 100 // these keys get a second event

	source := SourceFunc(func(ctx context.Context, emit func(Event) error) error {
		ts := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
		for i := 0; i < totalKeys; i++ {
			if err := emit(Event{Key: fmt.Sprintf("key-%d", i), Value: "v", Timestamp: ts}); err != nil {
				return err
			}
		}
		for i := 0; i < repeats; i++ {
			if err := emit(Event{Key: fmt.Sprintf("key-%d", i), Value: "v", Timestamp: ts}); err != nil {
				return err
			}
		}
		return nil
	})

	sink := &countingSink{}
	graph := NewGraph(source,
		func(event Event) Event { return event },
		func(event Event) string { return event.Key },
		sink)
	graph.SetParallelism(8)
	graph.SetProcess(runningCountProcess)

	if err := graph.Run(context.Background()); err != nil {
		t.Fatalf("run graph: %v", err)
	}

	if got := sink.n.Load(); got != int64(totalKeys+repeats) {
		t.Fatalf("expected %d emitted events, got %d", totalKeys+repeats, got)
	}

	// Every key holds state exactly once across all subtasks.
	totalStateKeys := 0
	for _, sub := range graph.State().Subtasks {
		totalStateKeys += sub["running-count"]
	}
	if totalStateKeys != totalKeys {
		t.Fatalf("expected state for %d keys, got %d", totalKeys, totalStateKeys)
	}

	// No cross-key bleed: repeated keys counted 2, others counted 1.
	for _, st := range graph.subs {
		repeated := ValueOf[int64](&StateContext{store: st.store, key: "key-0"}, "running-count")
		if got, ok := repeated.Get(); ok {
			if got != 2 {
				t.Fatalf("repeated key expected count 2, got %d", got)
			}
			other := ValueOf[int64](&StateContext{store: st.store, key: "key-500000"}, "running-count")
			if got, ok := other.Get(); ok && got != 1 {
				t.Fatalf("single-event key expected count 1, got %d", got)
			}
		}
	}
}

func TestDedupFiltersAlreadySeenIDs(t *testing.T) {
	t.Parallel()

	const keys = 10
	const uniqueOrdersPerKey = 50

	// Each key sees order-0..order-49, each exactly twice.
	source := SourceFunc(func(ctx context.Context, emit func(Event) error) error {
		ts := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
		for rep := 0; rep < 2; rep++ {
			for k := 0; k < keys; k++ {
				for o := 0; o < uniqueOrdersPerKey; o++ {
					if err := emit(Event{
						Key:       fmt.Sprintf("user-%d", k),
						Value:     fmt.Sprintf("order-%d", o),
						Timestamp: ts,
					}); err != nil {
						return err
					}
				}
			}
		}
		return nil
	})

	dedup := func(ctx *StateContext, event Event) ([]Event, error) {
		seen := MapOf[string, bool](ctx, "seen-orders")
		if _, ok := seen.Get(event.Value); ok {
			return nil, nil
		}
		seen.Put(event.Value, true)
		return []Event{event}, nil
	}

	sink := &countingSink{}
	graph := NewGraph(source,
		func(event Event) Event { return event },
		func(event Event) string { return event.Key },
		sink)
	graph.SetParallelism(4)
	graph.SetProcess(dedup)

	if err := graph.Run(context.Background()); err != nil {
		t.Fatalf("run graph: %v", err)
	}

	want := int64(keys * uniqueOrdersPerKey)
	if got := sink.n.Load(); got != want {
		t.Fatalf("expected %d unique events through, got %d", want, got)
	}

	totalStateKeys := 0
	for _, sub := range graph.State().Subtasks {
		totalStateKeys += sub["seen-orders"]
	}
	if totalStateKeys != keys {
		t.Fatalf("expected dedup state for %d keys, got %d", keys, totalStateKeys)
	}
}
