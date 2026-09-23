package runtime

import (
	"context"
	"sync"
	"testing"
	"time"
)

type recordedEvent struct {
	event Event
	key   string
}

type recordingSink struct {
	mu     sync.Mutex
	events []recordedEvent
	keys   map[string]int
	block  chan struct{}
}

func (s *recordingSink) Write(ctx context.Context, event Event) error {
	if s.block != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.block:
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, recordedEvent{event: event, key: event.Key})
	if s.keys == nil {
		s.keys = make(map[string]int)
	}
	s.keys[event.Key]++
	return nil
}

func TestGraphRoutesSameKeyToSameSubtask(t *testing.T) {
	t.Parallel()

	source := SourceFunc(func(ctx context.Context, emit func(Event) error) error {
		events := []Event{
			{Key: "alpha", Value: "1", Timestamp: time.Now()},
			{Key: "beta", Value: "2", Timestamp: time.Now()},
			{Key: "alpha", Value: "3", Timestamp: time.Now()},
			{Key: "gamma", Value: "4", Timestamp: time.Now()},
			{Key: "alpha", Value: "5", Timestamp: time.Now()},
		}
		for _, event := range events {
			if err := emit(event); err != nil {
				return err
			}
		}
		return nil
	})

	sink := &recordingSink{}
	graph := NewGraph(source, func(event Event) Event { return event }, func(event Event) string { return event.Key }, sink)
	graph.SetParallelism(4)
	graph.SetChannelCapacity(2)

	if err := graph.Run(context.Background()); err != nil {
		t.Fatalf("run graph: %v", err)
	}

	if got := sink.keys["alpha"]; got != 3 {
		t.Fatalf("expected alpha count 3, got %d", got)
	}
}

func TestGraphBackpressureBlocksSource(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	release := make(chan struct{})
	source := SourceFunc(func(ctx context.Context, emit func(Event) error) error {
		close(started)
		for i := 0; i < 4; i++ {
			if err := emit(Event{Key: "same", Value: "v", Timestamp: time.Now()}); err != nil {
				return err
			}
		}
		return nil
	})

	sink := &recordingSink{block: release}
	graph := NewGraph(source, func(event Event) Event { return event }, func(event Event) string { return event.Key }, sink)
	graph.SetParallelism(1)
	graph.SetChannelCapacity(1)

	done := make(chan error, 1)
	go func() {
		done <- graph.Run(context.Background())
	}()

	<-started

	select {
	case err := <-done:
		t.Fatalf("graph finished before sink was released: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run graph: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("graph did not finish after release")
	}
}

func TestGraphRequiresKeyBy(t *testing.T) {
	t.Parallel()

	graph := NewGraph(
		SourceFunc(func(ctx context.Context, emit func(Event) error) error { return nil }),
		func(event Event) Event { return event },
		nil,
		SinkFunc(func(ctx context.Context, event Event) error { return nil }),
	)

	err := graph.Run(context.Background())
	if err == nil {
		t.Fatal("expected keyBy error, got nil")
	}
	if err.Error() != "graph keyBy function is nil" {
		t.Fatalf("expected keyBy error, got %v", err)
	}
}
