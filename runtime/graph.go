package runtime

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
)

const defaultChannelCapacity = 16

// KeyFunc extracts the partitioning key from an event.
type KeyFunc func(Event) string

// Graph is a minimal in-process DAG for slice 1.
type Graph struct {
	source Source
	mapFn  MapFunc
	keyBy  KeyFunc
	sink   Sink

	parallelism     int
	channelCapacity int
}

// NewGraph builds a source -> map -> keyBy -> sink graph.
func NewGraph(source Source, mapFn MapFunc, keyBy KeyFunc, sink Sink) *Graph {
	return &Graph{
		source:          source,
		mapFn:           mapFn,
		keyBy:           keyBy,
		sink:            sink,
		parallelism:     1,
		channelCapacity: defaultChannelCapacity,
	}
}

// SetParallelism configures the number of downstream subtasks after keyBy.
func (g *Graph) SetParallelism(parallelism int) {
	if parallelism > 0 {
		g.parallelism = parallelism
	}
}

// SetChannelCapacity configures the bounded channel size per edge.
func (g *Graph) SetChannelCapacity(capacity int) {
	if capacity > 0 {
		g.channelCapacity = capacity
	}
}

// Run executes the graph.
func (g *Graph) Run(ctx context.Context) error {
	if g.source == nil {
		return errors.New("graph source is nil")
	}
	if g.mapFn == nil {
		return errors.New("graph map function is nil")
	}
	if g.keyBy == nil {
		return errors.New("graph keyBy function is nil")
	}
	if g.sink == nil {
		return errors.New("graph sink is nil")
	}

	type keyedEvent struct {
		event Event
		key   string
	}

	channels := make([]chan keyedEvent, g.parallelism)
	for i := range channels {
		channels[i] = make(chan keyedEvent, g.channelCapacity)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, g.parallelism+1)

	for i := 0; i < g.parallelism; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for item := range channels[idx] {
				if err := g.sink.Write(ctx, item.event); err != nil {
					errCh <- fmt.Errorf("sink subtask %d: %w", idx, err)
					return
				}
			}
		}(i)
	}

	sourceErr := g.source.Run(ctx, func(event Event) error {
		mapped := g.mapFn(event)
		key := g.keyBy(mapped)
		idx := routeKey(key, g.parallelism)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case channels[idx] <- keyedEvent{event: mapped, key: key}:
			return nil
		}
	})

	for _, ch := range channels {
		close(ch)
	}
	wg.Wait()

	if sourceErr != nil {
		return sourceErr
	}

	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func routeKey(key string, parallelism int) int {
	if parallelism <= 1 {
		return 0
	}

	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(parallelism))
}
