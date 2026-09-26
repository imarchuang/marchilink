package runtime

import (
	"context"
	"testing"
	"time"
)

// A savepoint is a named, manually triggered snapshot: take one mid-stream,
// "crash", resume from it by name, and the combined output must equal a
// no-crash run exactly.
func TestSavepointResumeMatchesNoCrashRun(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	events := crashTestEvents(base, 30)

	// Ground truth: no crash, no snapshots.
	truthSink := &collectSink{}
	truth := windowedCountJob(events, 0, "", 0, truthSink)
	if err := truth.Run(context.Background()); err != nil {
		t.Fatalf("no-crash run: %v", err)
	}
	want := truthSink.sorted()

	dir := t.TempDir()

	// Phase A: run slowly with the state backend on but NO periodic
	// checkpoints, take savepoint "sp1" mid-stream, then "crash".
	phaseASink := &collectSink{}
	crashCtx, cancel := context.WithCancel(context.Background())
	phaseA := windowedCountJob(events, 20*time.Millisecond, dir, 0, phaseASink)

	runDone := make(chan struct{})
	go func() {
		_ = phaseA.Run(crashCtx)
		close(runDone)
	}()

	// ~15 events in: trigger the savepoint and wait for it to complete.
	time.Sleep(300 * time.Millisecond)
	spCtx, spCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer spCancel()
	meta, err := phaseA.TriggerSavepoint(spCtx, "sp1")
	if err != nil {
		t.Fatalf("trigger savepoint: %v", err)
	}
	cancel()
	<-runDone

	// The savepoint lives under savepoints/sp1, same layout as a checkpoint.
	spMeta, srcState, _, err := newSavepointStore(dir, "sp1").loadLatest()
	if err != nil {
		t.Fatalf("load savepoint: %v", err)
	}
	if spMeta.ID != meta.ID {
		t.Fatalf("savepoint id mismatch: triggered %d, on disk %d", meta.ID, spMeta.ID)
	}
	t.Logf("savepoint %d at source offset %d", spMeta.ID, srcState.Offset)

	// Phase B: resume from the savepoint into a fresh sink, run to completion.
	phaseBSink := &collectSink{}
	resumed := windowedCountJob(events, 0, dir, 0, phaseBSink)
	resumed.SetResumeFrom("sp1")
	if err := resumed.Run(context.Background()); err != nil {
		t.Fatalf("resume run: %v", err)
	}

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

// Savepoints need the state backend; without a data directory the trigger
// fails fast instead of silently doing nothing.
func TestSavepointRequiresStateBackend(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	sink := &collectSink{}
	g := windowedCountJob(crashTestEvents(base, 5), 0, "", 0, sink)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := g.TriggerSavepoint(ctx, "nope"); err == nil {
		t.Fatal("expected error triggering savepoint without state backend")
	}
}
