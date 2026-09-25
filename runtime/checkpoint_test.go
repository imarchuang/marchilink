package runtime

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

// collectSink records all emitted values in order.
type collectSink struct {
	mu  chan struct{}
	out []string
}

func (s *collectSink) Write(_ context.Context, e Event) error {
	s.out = append(s.out, e.Key+"="+e.Value)
	return nil
}

func (s *collectSink) sorted() []string {
	cp := append([]string(nil), s.out...)
	sort.Strings(cp)
	return cp
}

// windowedCountJob builds the standard windowed-count graph for crash tests.
func windowedCountJob(events []Event, interval time.Duration, dataDir string, chkEvery time.Duration, sink Sink) *Graph {
	g := NewGraph(
		&ScriptedSource{Events: events, Interval: interval},
		func(e Event) Event { return e },
		func(e Event) string { return e.Key },
		sink,
	)
	g.SetParallelism(2)
	g.SetWatermarkBound(2 * time.Second)
	g.SetWindow(Tumbling(10*time.Second), Count())
	if dataDir != "" {
		g.SetCheckpointing(dataDir, "crash-test", chkEvery)
	}
	return g
}

func crashTestEvents(base time.Time, n int) []Event {
	events := make([]Event, 0, n)
	for i := 0; i < n; i++ {
		events = append(events, Event{
			Key:       fmt.Sprintf("key-%d", i%3),
			Value:     "1",
			Timestamp: base.Add(time.Duration(i) * time.Second),
		})
	}
	return events
}

func TestCheckpointWriteAndLoad(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store := newCheckpointStore(dir, "job")

	meta := checkpointMeta{ID: 1, JobID: "job", Parallelism: 2, Status: "completed", StartedAt: time.Now(), FinishedAt: time.Now()}
	src := sourceState{Offset: 42}
	subs := []subtaskSnapshot{
		{Clock: time.Now(), ProcessState: map[string]map[string]any{}, WindowState: map[string]map[string]aggState{}},
		{Clock: time.Now(), ProcessState: map[string]map[string]any{}, WindowState: map[string]map[string]aggState{}},
	}

	if err := store.write(meta, src, subs); err != nil {
		t.Fatalf("write: %v", err)
	}

	// LATEST pointer exists.
	if _, err := os.Stat(filepath.Join(dir, "checkpoints", "job", "LATEST")); err != nil {
		t.Fatalf("LATEST missing: %v", err)
	}

	gotMeta, gotSrc, gotSubs, err := store.loadLatest()
	if err != nil {
		t.Fatalf("loadLatest: %v", err)
	}
	if gotMeta.ID != 1 || gotSrc.Offset != 42 || len(gotSubs) != 2 {
		t.Fatalf("unexpected loaded checkpoint: meta=%+v src=%+v subs=%d", gotMeta, gotSrc, len(gotSubs))
	}
}

func TestCrashRestartCountsMatchNoCrashRun(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	events := crashTestEvents(base, 30)

	// --- Run 1: no crash, no checkpoint. Ground truth. ---
	truthSink := &collectSink{}
	truth := windowedCountJob(events, 0, "", 0, truthSink)
	if err := truth.Run(context.Background()); err != nil {
		t.Fatalf("no-crash run: %v", err)
	}
	want := truthSink.sorted()

	// --- Run 2: checkpoint, crash mid-stream, restart, finish. ---
	dir := t.TempDir()

	// Phase A: process events slowly so a checkpoint completes, then "crash".
	phaseASink := &collectSink{}
	crashCtx, cancel := context.WithCancel(context.Background())

	crashed := windowedCountJob(events, 20*time.Millisecond, dir, 100*time.Millisecond, phaseASink)

	// Cancel after ~15 events (enough for at least one checkpoint).
	go func() {
		time.Sleep(350 * time.Millisecond)
		cancel()
	}()
	_ = crashed.Run(crashCtx)

	// A checkpoint must have completed before the cancel.
	meta, srcState, _, loadErr := newCheckpointStore(dir, "crash-test").loadLatest()
	if loadErr != nil {
		t.Fatalf("expected a completed checkpoint before crash: %v", loadErr)
	}
	t.Logf("checkpoint %d completed at source offset %d", meta.ID, srcState.Offset)

	// Phase B: restart from the checkpoint into a FRESH sink, run to completion.
	// The restarted job replays events after the checkpoint offset; state was
	// rolled back to the same point, so windowed counts must match exactly.
	phaseBSink := &collectSink{}
	restarted := windowedCountJob(events, 0, dir, 100*time.Millisecond, phaseBSink)
	if err := restarted.Run(context.Background()); err != nil {
		t.Fatalf("restart run: %v", err)
	}

	// Combine: phase A emitted windows that fired before the crash, phase B
	// emitted the rest. Together they must equal the no-crash run.
	combined := &collectSink{out: append(append([]string(nil), phaseASink.out...), phaseBSink.out...)}
	got := combined.sorted()

	if len(got) != len(want) {
		t.Fatalf("result count mismatch: got %d, want %d\ngot:  %v\nwant: %v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("result mismatch at %d:\ngot:  %v\nwant: %v", i, got, want)
		}
	}
}

func TestRestoreRewindsSource(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	base := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	events := crashTestEvents(base, 20)

	// Run once with checkpointing to produce a checkpoint.
	sink1 := &collectSink{}
	g1 := windowedCountJob(events, 10*time.Millisecond, dir, 50*time.Millisecond, sink1)
	if err := g1.Run(context.Background()); err != nil {
		t.Fatalf("first run: %v", err)
	}

	// Verify a checkpoint exists.
	_, srcState, _, err := newCheckpointStore(dir, "crash-test").loadLatest()
	if err != nil {
		t.Fatalf("no checkpoint: %v", err)
	}
	t.Logf("checkpointed at source offset %d", srcState.Offset)

	// Create a fresh source and verify Seek works.
	src := &ScriptedSource{Events: events}
	src.Seek(srcState.Offset)
	if got := src.Offset(); got != srcState.Offset {
		t.Fatalf("after Seek(%d), Offset()=%d", srcState.Offset, got)
	}
}
