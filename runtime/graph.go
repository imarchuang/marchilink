package runtime

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

const defaultChannelCapacity = 16

// finalWatermark flushes all remaining windows when a bounded source ends.
// It is never fed into any tracker; it only triggers a final fire.
var finalWatermark = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)

// KeyFunc extracts the partitioning key from an event.
type KeyFunc func(Event) string

// WatermarkSnapshot reports watermark and window progress for the graph.
type WatermarkSnapshot struct {
	Source         time.Time   `json:"source"`
	Subtasks       []time.Time `json:"subtasks"`
	Min            time.Time   `json:"min"`
	LateRecords    int64       `json:"lateRecords"`
	PendingWindows int64       `json:"pendingWindows"`
}

// ProcessFunc is a stateful flat-map running inside a keyed subtask. State
// accessed through the StateContext is scoped to the key of the input event.
// It returns the events to emit downstream (empty slice = filter out).
type ProcessFunc func(ctx *StateContext, event Event) ([]Event, error)

// Graph is a minimal in-process DAG: source -> map -> keyBy -> process -> window -> sink.
type Graph struct {
	source Source
	mapFn  MapFunc
	keyBy  KeyFunc
	sink   Sink

	processFn ProcessFunc
	assigner  WindowAssigner
	agg       AggFactory

	parallelism     int
	channelCapacity int
	watermarkBound  time.Duration

	mu       sync.RWMutex
	sourceWM *WatermarkTracker
	subs     []*windowSubtask
}

// windowSubtask is one parallel instance of the keyed operators: its own
// event-time clock, its own keyed state store, and its own slice of window
// state.
type windowSubtask struct {
	tracker *WatermarkTracker
	store   *stateStore
	sctx    *StateContext
	states  map[string]map[Window]Aggregator
	pending atomic.Int64
	late    atomic.Int64
}

// streamMsg is what flows through a channel: either a data record or a
// watermark control record (broadcast to every subtask).
type streamMsg struct {
	event Event
	key   string
	wm    time.Time
	isWM  bool
	final bool
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
		watermarkBound:  5 * time.Second,
	}
}

// SetParallelism configures the number of window subtasks after keyBy.
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

// SetWatermarkBound configures bounded out-of-orderness at the source.
func (g *Graph) SetWatermarkBound(bound time.Duration) {
	if bound >= 0 {
		g.watermarkBound = bound
	}
}

// SetWindow enables the window operator. Without it, subtasks pass events
// straight through to the sink.
func (g *Graph) SetWindow(assigner WindowAssigner, agg AggFactory) {
	g.assigner = assigner
	g.agg = agg
}

// SetProcess enables the stateful process operator between keyBy and window.
func (g *Graph) SetProcess(fn ProcessFunc) {
	g.processFn = fn
}

// Watermarks returns the current watermark/window snapshot.
func (g *Graph) Watermarks() WatermarkSnapshot {
	g.mu.RLock()
	defer g.mu.RUnlock()

	snapshot := WatermarkSnapshot{}
	if g.sourceWM != nil {
		snapshot.Source = g.sourceWM.Current()
	}
	for _, st := range g.subs {
		clock := st.tracker.Current()
		snapshot.Subtasks = append(snapshot.Subtasks, clock)
		if snapshot.Min.IsZero() || (!clock.IsZero() && clock.Before(snapshot.Min)) {
			snapshot.Min = clock
		}
		snapshot.LateRecords += st.late.Load()
		snapshot.PendingWindows += st.pending.Load()
	}
	return snapshot
}

// StateSnapshot reports keyed state sizes per subtask: state name -> key count.
type StateSnapshot struct {
	Subtasks []map[string]int `json:"subtasks"`
}

// State returns the current keyed-state snapshot.
func (g *Graph) State() StateSnapshot {
	g.mu.RLock()
	defer g.mu.RUnlock()

	snap := StateSnapshot{}
	for _, st := range g.subs {
		snap.Subtasks = append(snap.Subtasks, st.store.keyCounts())
	}
	return snap
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
	if (g.assigner == nil) != (g.agg == nil) {
		return errors.New("window assigner and aggregator must be set together")
	}

	subChs := make([]chan streamMsg, g.parallelism)
	subs := make([]*windowSubtask, g.parallelism)
	for i := range subChs {
		subChs[i] = make(chan streamMsg, g.channelCapacity)
		store := newStateStore()
		subs[i] = &windowSubtask{
			tracker: NewWatermarkTracker(1, 0),
			store:   store,
			sctx:    &StateContext{store: store},
			states:  make(map[string]map[Window]Aggregator),
		}
	}
	sinkCh := make(chan Event, g.channelCapacity)

	g.mu.Lock()
	g.sourceWM = NewWatermarkTracker(1, g.watermarkBound)
	g.subs = subs
	g.mu.Unlock()

	// Single sink goroutine; drains even after an error so subtasks never
	// block on a dead sink.
	sinkDone := make(chan error, 1)
	go func() {
		var sinkErr error
		for event := range sinkCh {
			if sinkErr == nil {
				if err := g.sink.Write(ctx, event); err != nil {
					sinkErr = err
				}
			}
		}
		sinkDone <- sinkErr
	}()

	var subWG sync.WaitGroup
	subErrs := make(chan error, g.parallelism)
	for i := 0; i < g.parallelism; i++ {
		subWG.Add(1)
		go func(idx int) {
			defer subWG.Done()
			if err := g.runSubtask(ctx, subs[idx], subChs[idx], sinkCh); err != nil {
				subErrs <- fmt.Errorf("window subtask %d: %w", idx, err)
			}
		}(i)
	}

	sourceErr := g.source.Run(ctx, func(event Event) error {
		mapped := g.mapFn(event)
		key := g.keyBy(mapped)
		idx := routeKey(key, g.parallelism)
		wm, advanced := g.sourceWM.ObserveEvent(mapped.Timestamp)

		if err := sendMsg(ctx, subChs[idx], streamMsg{event: mapped, key: key}); err != nil {
			return err
		}

		// Watermark advanced: broadcast it to every subtask, like Flink
		// broadcasts watermark control records on all output channels.
		if advanced {
			for _, ch := range subChs {
				if err := sendMsg(ctx, ch, streamMsg{isWM: true, wm: wm}); err != nil {
					return err
				}
			}
		}
		return nil
	})

	if sourceErr == nil {
		// Bounded source ended: flush every remaining window.
		for _, ch := range subChs {
			if err := sendMsg(ctx, ch, streamMsg{isWM: true, final: true, wm: finalWatermark}); err != nil {
				sourceErr = err
				break
			}
		}
	}

	for _, ch := range subChs {
		close(ch)
	}
	subWG.Wait()
	close(sinkCh)
	sinkErr := <-sinkDone

	if sourceErr != nil {
		return sourceErr
	}
	select {
	case err := <-subErrs:
		return err
	default:
	}
	return sinkErr
}

// runSubtask is the loop of one window subtask: data records update keyed
// window state, watermark records advance the subtask's own clock and fire
// due windows.
func (g *Graph) runSubtask(ctx context.Context, st *windowSubtask, in <-chan streamMsg, sinkCh chan<- Event) error {
	for m := range in {
		switch {
		case m.isWM && m.final:
			if err := g.fireWhere(ctx, st, sinkCh, func(Window) bool { return true }); err != nil {
				return err
			}
		case m.isWM:
			clock := st.tracker.UpdateInput(0, m.wm)
			if err := g.fireWhere(ctx, st, sinkCh, func(w Window) bool { return !clock.Before(w.End) }); err != nil {
				return err
			}
		default:
			events := []Event{m.event}
			if g.processFn != nil {
				st.sctx.key = m.key
				out, err := g.processFn(st.sctx, m.event)
				if err != nil {
					return fmt.Errorf("process: %w", err)
				}
				events = out
			}

			for _, event := range events {
				if g.assigner == nil {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case sinkCh <- event:
					}
					continue
				}

				clock := st.tracker.Current()
				for _, w := range g.assigner.Assign(event.Timestamp) {
					if !w.End.After(clock) {
						// The window already fired: this record is late.
						st.late.Add(1)
						continue
					}
					byWindow := st.states[m.key]
					if byWindow == nil {
						byWindow = make(map[Window]Aggregator)
						st.states[m.key] = byWindow
					}
					agg, ok := byWindow[w]
					if !ok {
						agg = g.agg()
						byWindow[w] = agg
						st.pending.Add(1)
					}
					agg.Add(event)
				}
			}
		}
	}
	return nil
}

// fireWhere emits and deletes every window matching pred.
func (g *Graph) fireWhere(ctx context.Context, st *windowSubtask, sinkCh chan<- Event, pred func(Window) bool) error {
	for key, byWindow := range st.states {
		for w, agg := range byWindow {
			if !pred(w) {
				continue
			}
			result := Event{
				Key:       key,
				Value:     fmt.Sprintf("window[%s~%s) %s", w.Start.Format("15:04:05"), w.End.Format("15:04:05"), agg.Result()),
				Timestamp: w.End,
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case sinkCh <- result:
			}
			delete(byWindow, w)
			st.pending.Add(-1)
		}
		if len(byWindow) == 0 {
			delete(st.states, key)
		}
	}
	return nil
}

func sendMsg(ctx context.Context, ch chan<- streamMsg, m streamMsg) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ch <- m:
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
