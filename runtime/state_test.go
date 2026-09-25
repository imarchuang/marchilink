package runtime

import (
	"testing"
)

func TestValueStateScopedPerKey(t *testing.T) {
	t.Parallel()

	store := newStateStore()
	ctxA := &StateContext{store: store, key: "a"}
	ctxB := &StateContext{store: store, key: "b"}

	countA := ValueOf[int](ctxA, "count")
	countB := ValueOf[int](ctxB, "count")

	if _, ok := countA.Get(); ok {
		t.Fatal("expected unset state to report not-ok")
	}

	countA.Set(41)
	countA.Set(42)
	countB.Set(7)

	if got, _ := countA.Get(); got != 42 {
		t.Fatalf("key a: expected 42, got %d", got)
	}
	if got, _ := countB.Get(); got != 7 {
		t.Fatalf("key b: expected 7, got %d", got)
	}

	countA.Clear()
	if _, ok := countA.Get(); ok {
		t.Fatal("expected cleared state to report not-ok")
	}
	if got, _ := countB.Get(); got != 7 {
		t.Fatalf("key b must survive key a clear, got %d", got)
	}

	if counts := store.keyCounts(); counts["count"] != 1 {
		t.Fatalf("expected 1 remaining key for state 'count', got %v", counts)
	}
}

func TestListState(t *testing.T) {
	t.Parallel()

	store := newStateStore()
	ctx := &StateContext{store: store, key: "a"}

	list := ListOf[string](ctx, "recent")
	list.Add("x")
	list.Add("y")

	got := list.Get()
	if len(got) != 2 || got[0] != "x" || got[1] != "y" {
		t.Fatalf("unexpected list contents: %v", got)
	}

	list.Clear()
	if got := list.Get(); len(got) != 0 {
		t.Fatalf("expected empty list after clear, got %v", got)
	}
}

func TestMapState(t *testing.T) {
	t.Parallel()

	store := newStateStore()
	ctxA := &StateContext{store: store, key: "a"}
	ctxB := &StateContext{store: store, key: "b"}

	seenA := MapOf[string, bool](ctxA, "seen")
	seenB := MapOf[string, bool](ctxB, "seen")

	seenA.Put("order-1", true)
	seenA.Put("order-2", true)
	seenB.Put("order-1", true)

	if _, ok := seenA.Get("order-2"); !ok {
		t.Fatal("expected order-2 to be seen for key a")
	}
	if _, ok := seenB.Get("order-2"); ok {
		t.Fatal("order-2 must not leak into key b")
	}
	if got := seenA.Len(); got != 2 {
		t.Fatalf("expected len 2 for key a, got %d", got)
	}

	seenA.Remove("order-1")
	if _, ok := seenA.Get("order-1"); ok {
		t.Fatal("expected order-1 removed for key a")
	}
	if _, ok := seenB.Get("order-1"); !ok {
		t.Fatal("key b must survive key a remove")
	}
}
