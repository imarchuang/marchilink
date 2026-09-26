package runtime

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
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

	allowedLateness time.Duration
	sideSink        Sink

	parallelism     int
	channelCapacity int
	watermarkBound  time.Duration

	// checkpointing
	jobID             string
	dataDir           string
	checkpointEvery   time.Duration
	checkpointEnabled bool
	// stateBackend is on whenever dataDir is set: the coordinator runs (for
	// savepoints and restore) even without periodic checkpoints.
	stateBackend bool
	resumeFrom   string

	mu       sync.RWMutex
	sourceWM *WatermarkTracker
	subs     []*windowSubtask
	store    *checkpointStore
	history  []checkpointMeta
	spReq    chan savepointRequest
}

// windowSubtask is one parallel instance of the keyed operators: its own
// event-time clock, its own keyed state store, and its own slice of window
// state.
type windowSubtask struct {
	tracker *WatermarkTracker
	store   *stateStore
	sctx    *StateContext
	states  map[string]map[Window]*windowCell
	pending atomic.Int64
	late    atomic.Int64
}

// windowCell is one (key, window) state cell: the aggregator plus whether
// the window has fired. Fired cells are kept until the allowed lateness
// expires, so late records can re-fire the window with an updated result.
type windowCell struct {
	agg   Aggregator
	fired bool
}

// streamMsg is what flows through a channel: a data record, a watermark
// control record, or a checkpoint barrier (all broadcast to every subtask).
type streamMsg struct {
	event Event
	key   string
	wm    time.Time
	isWM  bool
	final bool

	isBarrier   bool
	barrierID   checkpointID
	barrierSnap int64 // source offset carried by the barrier
}

// barrierAck is a subtask's acknowledgement that it snapshotted at a barrier.
type barrierAck struct {
	id           checkpointID
	idx          int
	snap         subtaskSnapshot
	sourceOffset int64
}

// savepointRequest asks the coordinator for a named, manually triggered
// snapshot. The coordinator replies on result when the savepoint completes.
type savepointRequest struct {
	name   string
	result chan savepointResult
}

type savepointResult struct {
	meta checkpointMeta
	err  error
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

// SetAllowedLateness keeps fired window state for d after the watermark
// passes the window end. Late records within d re-fire the window with an
// updated result; records later than that go to the side output (or are
// dropped and counted when no side output is set).
func (g *Graph) SetAllowedLateness(d time.Duration) {
	if d > 0 {
		g.allowedLateness = d
	}
}

// SetSideOutput sends too-late records to this sink instead of dropping them.
func (g *Graph) SetSideOutput(sink Sink) {
	g.sideSink = sink
}

// SetCheckpointing enables the state backend under {dataDir}: periodic
// checkpoints under checkpoints/{jobID} when every > 0, and on-demand
// savepoints under savepoints/{name} regardless of the interval.
func (g *Graph) SetCheckpointing(dataDir, jobID string, every time.Duration) {
	if dataDir == "" {
		return
	}
	g.dataDir = dataDir
	g.jobID = jobID
	g.store = newCheckpointStore(dataDir, jobID)
	g.stateBackend = true
	if every > 0 {
		g.checkpointEvery = every
		g.checkpointEnabled = true
	}
}

// SetResumeFrom makes the next Run restore from the named savepoint under
// {dataDir}/savepoints/{name} instead of the latest checkpoint.
func (g *Graph) SetResumeFrom(name string) {
	g.resumeFrom = name
}

// TriggerSavepoint asks the coordinator for a named savepoint and waits for
// it to complete. Savepoints use the checkpoint format but live under
// savepoints/{name} and are never auto-deleted.
func (g *Graph) TriggerSavepoint(ctx context.Context, name string) (checkpointMeta, error) {
	g.mu.RLock()
	ch := g.spReq
	enabled := g.stateBackend
	g.mu.RUnlock()
	if !enabled {
		return checkpointMeta{}, errors.New("state backend disabled: run with a data directory")
	}
	if ch == nil {
		return checkpointMeta{}, errors.New("job is not running")
	}

	req := savepointRequest{name: name, result: make(chan savepointResult, 1)}
	select {
	case ch <- req:
	case <-ctx.Done():
		return checkpointMeta{}, ctx.Err()
	}
	select {
	case res := <-req.result:
		return res.meta, res.err
	case <-ctx.Done():
		return checkpointMeta{}, ctx.Err()
	}
}

// Checkpoints returns completed checkpoint metadata, oldest first.
func (g *Graph) Checkpoints() []checkpointMeta {
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make([]checkpointMeta, len(g.history))
	copy(out, g.history)
	return out
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
			states:  make(map[string]map[Window]*windowCell),
		}
	}
	sinkCh := make(chan Event, g.channelCapacity)

	// Side output for too-late records; same drain-on-error pattern as the
	// main sink so subtasks never block on a dead sink.
	var sideCh chan Event
	sideDone := make(chan error, 1)
	if g.sideSink != nil {
		sideCh = make(chan Event, g.channelCapacity)
		go func() {
			var sideErr error
			for event := range sideCh {
				if sideErr == nil {
					if err := g.sideSink.Write(ctx, event); err != nil {
						sideErr = err
					}
				}
			}
			sideDone <- sideErr
		}()
	} else {
		close(sideDone)
	}

	g.mu.Lock()
	g.sourceWM = NewWatermarkTracker(1, g.watermarkBound)
	g.subs = subs
	g.mu.Unlock()

	// Restore from the latest completed checkpoint — or from the named
	// savepoint when resuming manually.
	var startOffset int64
	if g.stateBackend {
		restoreStore := g.store
		if g.resumeFrom != "" {
			restoreStore = newSavepointStore(g.dataDir, g.resumeFrom)
		}
		if err := g.restoreFrom(restoreStore, subs); err != nil {
			return fmt.Errorf("restore: %w", err)
		}
		if off, ok := g.source.(interface{ Offset() int64 }); ok {
			startOffset = off.Offset()
		}
		// For marchiq sources, restore per-partition offsets from the same store.
		if _, srcState, _, err := restoreStore.loadLatest(); err == nil && len(srcState.Offsets) > 0 {
			if restorer, ok := g.source.(interface{ RestoreOffsets(map[int]int64) }); ok {
				restorer.RestoreOffsets(srcState.Offsets)
			}
		}
	}

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

	ackCh := make(chan barrierAck, g.parallelism*4)

	var subWG sync.WaitGroup
	subErrs := make(chan error, g.parallelism)
	for i := 0; i < g.parallelism; i++ {
		subWG.Add(1)
		go func(idx int) {
			defer subWG.Done()
			if err := g.runSubtask(ctx, subs[idx], subChs[idx], sinkCh, sideCh, ackCh); err != nil {
				subErrs <- fmt.Errorf("window subtask %d: %w", idx, err)
			}
		}(i)
	}

	// Checkpoint coordinator: periodically asks the source to inject a barrier.
	// barrierReq carries the barrier id; the source replies on barrierInjected
	// with the offset it snapshotted, so the coordinator knows the barrier is
	// in the stream before waiting for acks.
	barrierReq := make(chan checkpointID)
	barrierInjected := make(chan int64) // source offset at barrier
	savepointReqs := make(chan savepointRequest)
	coordDone := make(chan struct{})
	sourceDone := make(chan struct{})
	if g.stateBackend {
		g.mu.Lock()
		g.spReq = savepointReqs
		g.mu.Unlock()
		go g.runCoordinator(ctx, barrierReq, barrierInjected, ackCh, savepointReqs, coordDone, sourceDone)
	} else {
		close(coordDone)
	}

	var sourceOffset int64 = startOffset
	sourceErr := g.source.Run(ctx, func(event Event) error {
		mapped := g.mapFn(event)
		key := g.keyBy(mapped)
		idx := routeKey(key, g.parallelism)
		wm, advanced := g.sourceWM.ObserveEvent(mapped.Timestamp)

		if err := sendMsg(ctx, subChs[idx], streamMsg{event: mapped, key: key}); err != nil {
			return err
		}
		sourceOffset++

		// Watermark advanced: broadcast it to every subtask, like Flink
		// broadcasts watermark control records on all output channels.
		if advanced {
			for _, ch := range subChs {
				if err := sendMsg(ctx, ch, streamMsg{isWM: true, wm: wm}); err != nil {
					return err
				}
			}
		}

		// If the coordinator asked for a barrier, inject it now: the barrier
		// carries the offset of the NEXT record, i.e. all records before it
		// have already been emitted.
		select {
		case id := <-barrierReq:
			for _, ch := range subChs {
				if err := sendMsg(ctx, ch, streamMsg{isBarrier: true, barrierID: id, barrierSnap: sourceOffset}); err != nil {
					return err
				}
			}
			select {
			case barrierInjected <- sourceOffset:
			case <-ctx.Done():
				return ctx.Err()
			}
		default:
		}
		return nil
	})
	close(sourceDone)

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
	if sideCh != nil {
		close(sideCh)
	}
	if sideErr := <-sideDone; sinkErr == nil {
		sinkErr = sideErr
	}

	// Stop the coordinator.
	if g.stateBackend {
		close(barrierReq)
	}
	<-coordDone

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

// errSourceEnded marks snapshot attempts that lose the race with a bounded
// source finishing.
var errSourceEnded = errors.New("source ended")

// runCoordinator triggers a checkpoint every interval and serves savepoint
// requests. Each round injects a barrier, waits for all subtask acks, then
// atomically publishes the snapshot. With checkpointing disabled the ticker
// channel is nil and only savepoints are served.
func (g *Graph) runCoordinator(ctx context.Context, barrierReq chan checkpointID, barrierInjected <-chan int64, ackCh <-chan barrierAck, spReq <-chan savepointRequest, done chan<- struct{}, sourceDone <-chan struct{}) {
	defer close(done)

	var tickC <-chan time.Time
	if g.checkpointEnabled {
		ticker := time.NewTicker(g.checkpointEvery)
		defer ticker.Stop()
		tickC = ticker.C
	}

	var nextID checkpointID = 1
	// If we restored from a checkpoint, continue numbering after it.
	if meta, _, _, err := g.store.loadLatest(); err == nil {
		nextID = meta.ID + 1
	}

	sourceEnded := false
	for {
		if sourceEnded {
			// No more snapshots are possible: subtasks have drained. Stay
			// alive to reject savepoint requests until Run shuts us down by
			// closing barrierReq.
			select {
			case <-ctx.Done():
				return
			case <-barrierReq:
				return
			case sp := <-spReq:
				sp.result <- savepointResult{err: errSourceEnded}
			}
			continue
		}

		var sp *savepointRequest
		select {
		case <-ctx.Done():
			return
		case <-sourceDone:
			sourceEnded = true
			continue
		case <-tickC:
		case req := <-spReq:
			sp = &req
		}

		meta, src, snaps, err := g.snapshotOnce(ctx, nextID, barrierReq, barrierInjected, ackCh, sourceDone)
		if err != nil {
			if errors.Is(err, errSourceEnded) {
				sourceEnded = true
				if sp != nil {
					sp.result <- savepointResult{err: err}
				}
				continue
			}
			return // ctx cancelled
		}

		// Periodic checkpoints go to the checkpoint store; savepoints go to
		// their named directory and are never auto-deleted.
		store := g.store
		if sp != nil {
			store = newSavepointStore(g.dataDir, sp.name)
		}
		if err := store.write(meta, src, snaps); err != nil {
			// A failed snapshot must not kill the job.
			if sp != nil {
				sp.result <- savepointResult{err: fmt.Errorf("write savepoint: %w", err)}
			}
			continue
		}

		// Snapshot completed: commit source offsets to marchiq.
		if committer, ok := g.source.(interface{ CommitOffsets(context.Context) error }); ok {
			// On failure, offsets are re-committed on the next checkpoint or
			// replayed on recovery.
			_ = committer.CommitOffsets(ctx)
		}

		nextID++
		if sp != nil {
			sp.result <- savepointResult{meta: meta}
			continue
		}
		g.mu.Lock()
		g.history = append(g.history, meta)
		g.mu.Unlock()
	}
}

// snapshotOnce runs one barrier round: the source injects the barrier after
// the current record, every subtask acks with its snapshot, and the captured
// state is returned. Nothing is written yet — the caller decides where to
// publish (checkpoint store vs named savepoint).
func (g *Graph) snapshotOnce(ctx context.Context, id checkpointID, barrierReq chan<- checkpointID, barrierInjected <-chan int64, ackCh <-chan barrierAck, sourceDone <-chan struct{}) (checkpointMeta, sourceState, []subtaskSnapshot, error) {
	started := time.Now()

	// Ask the source to inject a barrier, then wait for it to confirm.
	select {
	case barrierReq <- id:
	case <-ctx.Done():
		return checkpointMeta{}, sourceState{}, nil, ctx.Err()
	case <-sourceDone:
		return checkpointMeta{}, sourceState{}, nil, errSourceEnded
	}
	var srcOffset int64
	select {
	case srcOffset = <-barrierInjected:
	case <-ctx.Done():
		return checkpointMeta{}, sourceState{}, nil, ctx.Err()
	case <-sourceDone:
		return checkpointMeta{}, sourceState{}, nil, errSourceEnded
	}

	// For marchiq sources, snapshot per-partition offsets.
	var srcOffsets map[int]int64
	if om, ok := g.source.(interface{ OffsetMap() map[int]int64 }); ok {
		srcOffsets = om.OffsetMap()
	}

	// Wait for every subtask to ack with its snapshot. The barrier is already
	// in the channels, so acks arrive even if the source ends meanwhile.
	snaps := make([]subtaskSnapshot, g.parallelism)
	acked := 0
	for acked < g.parallelism {
		select {
		case <-ctx.Done():
			return checkpointMeta{}, sourceState{}, nil, ctx.Err()
		case ack := <-ackCh:
			if ack.id != id {
				continue
			}
			snaps[ack.idx] = ack.snap
			acked++
		}
	}

	meta := checkpointMeta{
		ID:          id,
		JobID:       g.jobID,
		StartedAt:   started,
		FinishedAt:  time.Now(),
		Parallelism: g.parallelism,
		Status:      "completed",
	}
	return meta, sourceState{Offset: srcOffset, Offsets: srcOffsets}, snaps, nil
}

// restoreFrom loads the latest completed snapshot in store into subtask state
// and rewinds the source.
func (g *Graph) restoreFrom(store *checkpointStore, subs []*windowSubtask) error {
	meta, src, snaps, err := store.loadLatest()
	if err != nil {
		if os.IsNotExist(err) {
			return nil // no checkpoint yet
		}
		return err
	}
	if int(meta.Parallelism) != g.parallelism {
		return fmt.Errorf("checkpoint parallelism %d != job parallelism %d", meta.Parallelism, g.parallelism)
	}

	for i := range subs {
		snap := snaps[i]
		subs[i].tracker.Restore(snap.Clock)
		subs[i].store.restore(snap.ProcessState)
		subs[i].late.Store(snap.Late)
		subs[i].pending.Store(snap.Pending)

		states := make(map[string]map[Window]*windowCell)
		for key, byWindow := range snap.WindowState {
			wm := make(map[Window]*windowCell, len(byWindow))
			for wk, as := range byWindow {
				w, err := parseWindowKey(wk)
				if err != nil {
					return err
				}
				agg := g.agg()
				if agg.snapshotKind() != as.Kind {
					return fmt.Errorf("aggregator kind mismatch: %s vs %s", agg.snapshotKind(), as.Kind)
				}
				if err := agg.restore(as.Value); err != nil {
					return err
				}
				wm[w] = &windowCell{agg: agg, fired: as.Fired}
			}
			states[key] = wm
		}
		subs[i].states = states
	}

	// Rewind the source.
	if seeker, ok := g.source.(interface{ Seek(offset int64) }); ok {
		seeker.Seek(src.Offset)
	}
	return nil
}

// runSubtask is the loop of one window subtask: data records update keyed
// window state, watermark records advance the subtask's own clock and fire
// due windows, barrier records trigger a state snapshot.
func (g *Graph) runSubtask(ctx context.Context, st *windowSubtask, in <-chan streamMsg, sinkCh, sideCh chan<- Event, ackCh chan<- barrierAck) error {
	for m := range in {
		switch {
		case m.isBarrier:
			// Barrier arrived on our (single) input: snapshot state now.
			snap := subtaskSnapshot{
				Clock:        st.tracker.Current(),
				ProcessState: st.store.snapshot(),
				WindowState:  snapshotWindowState(st.states),
				Late:         st.late.Load(),
				Pending:      st.pending.Load(),
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case ackCh <- barrierAck{id: m.barrierID, idx: subtaskIndex(g, st), snap: snap, sourceOffset: m.barrierSnap}:
			}
		case m.isWM && m.final:
			// Bounded source ended: fire and purge everything.
			if err := g.fireDue(ctx, st, sinkCh, finalWatermark); err != nil {
				return err
			}
			g.purgeExpired(st, finalWatermark)
		case m.isWM:
			clock := st.tracker.UpdateInput(0, m.wm)
			if err := g.fireDue(ctx, st, sinkCh, clock); err != nil {
				return err
			}
			g.purgeExpired(st, clock)
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
					byWindow := st.states[m.key]
					if byWindow == nil {
						byWindow = make(map[Window]*windowCell)
						st.states[m.key] = byWindow
					}

					if !w.End.After(clock) {
						// The window end has passed.
						if clock.Before(w.End.Add(g.allowedLateness)) {
							// Within allowed lateness: accept the record and
							// re-fire the window with the updated result.
							cell := byWindow[w]
							if cell == nil {
								cell = &windowCell{agg: g.agg()}
								byWindow[w] = cell
								st.pending.Add(1)
							}
							cell.agg.Add(event)
							cell.fired = true
							if err := g.emitWindow(ctx, sinkCh, m.key, w, cell); err != nil {
								return err
							}
							continue
						}
						// Too late: side output if configured, else drop.
						// Counted either way.
						st.late.Add(1)
						if sideCh != nil {
							select {
							case <-ctx.Done():
								return ctx.Err()
							case sideCh <- event:
							}
						}
						continue
					}

					cell := byWindow[w]
					if cell == nil {
						cell = &windowCell{agg: g.agg()}
						byWindow[w] = cell
						st.pending.Add(1)
					}
					cell.agg.Add(event)
				}
			}
		}
	}
	return nil
}

// fireDue emits every unfired window whose end the clock has passed and marks
// it fired. State is kept until purgeExpired reclaims it, so late records
// within the allowed lateness can re-fire with updated results.
func (g *Graph) fireDue(ctx context.Context, st *windowSubtask, sinkCh chan<- Event, clock time.Time) error {
	for key, byWindow := range st.states {
		for w, cell := range byWindow {
			if cell.fired || clock.Before(w.End) {
				continue
			}
			if err := g.emitWindow(ctx, sinkCh, key, w, cell); err != nil {
				return err
			}
			cell.fired = true
		}
	}
	return nil
}

// purgeExpired deletes windows whose end plus allowed lateness the clock has
// passed. With zero lateness this coincides with firing; with lateness the
// fired state lingers for re-fires until the purge horizon.
func (g *Graph) purgeExpired(st *windowSubtask, clock time.Time) {
	for key, byWindow := range st.states {
		for w := range byWindow {
			if clock.Before(w.End.Add(g.allowedLateness)) {
				continue
			}
			delete(byWindow, w)
			st.pending.Add(-1)
		}
		if len(byWindow) == 0 {
			delete(st.states, key)
		}
	}
}

// emitWindow sends one window result downstream.
func (g *Graph) emitWindow(ctx context.Context, sinkCh chan<- Event, key string, w Window, cell *windowCell) error {
	result := Event{
		Key:       key,
		Value:     fmt.Sprintf("window[%s~%s) %s", w.Start.Format("15:04:05"), w.End.Format("15:04:05"), cell.agg.Result()),
		Timestamp: w.End,
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case sinkCh <- result:
		return nil
	}
}

func sendMsg(ctx context.Context, ch chan<- streamMsg, m streamMsg) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ch <- m:
		return nil
	}
}

// subtaskIndex finds a subtask's index by identity (used for barrier acks).
func subtaskIndex(g *Graph, target *windowSubtask) int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	for i, st := range g.subs {
		if st == target {
			return i
		}
	}
	return -1
}

// snapshotWindowState converts live window cells to a serializable form.
func snapshotWindowState(states map[string]map[Window]*windowCell) map[string]map[string]aggState {
	out := make(map[string]map[string]aggState, len(states))
	for key, byWindow := range states {
		wm := make(map[string]aggState, len(byWindow))
		for w, cell := range byWindow {
			wm[windowKey(w)] = aggState{Kind: cell.agg.snapshotKind(), Value: cell.agg.Result(), Fired: cell.fired}
		}
		out[key] = wm
	}
	return out
}

func routeKey(key string, parallelism int) int {
	if parallelism <= 1 {
		return 0
	}

	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(parallelism))
}
