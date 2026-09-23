package runtime

import "sync"

// stateStore is the in-memory keyed state backend for one subtask.
// Layout: state name -> key -> state cell. Every cell is implicitly scoped
// to (operator, key): the name comes from the operator code, the key from
// the record currently being processed.
type stateStore struct {
	mu    sync.RWMutex
	cells map[string]map[string]any
}

func newStateStore() *stateStore {
	return &stateStore{cells: make(map[string]map[string]any)}
}

func (s *stateStore) lookup(name, key string) (any, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	byKey := s.cells[name]
	if byKey == nil {
		return nil, false
	}
	cell, ok := byKey[key]
	return cell, ok
}

func (s *stateStore) getOrCreate(name, key string, makeCell func() any) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	byKey := s.cells[name]
	if byKey == nil {
		byKey = make(map[string]any)
		s.cells[name] = byKey
	}
	cell, ok := byKey[key]
	if !ok {
		cell = makeCell()
		byKey[key] = cell
	}
	return cell
}

func (s *stateStore) clear(name, key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	byKey := s.cells[name]
	if byKey == nil {
		return
	}
	delete(byKey, key)
	if len(byKey) == 0 {
		delete(s.cells, name)
	}
}

// keyCounts reports state name -> number of keys holding that state.
func (s *stateStore) keyCounts() map[string]int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]int, len(s.cells))
	for name, byKey := range s.cells {
		out[name] = len(byKey)
	}
	return out
}

// StateContext scopes all state access to the key of the event currently
// being processed. It is only valid inside a ProcessFunc call.
type StateContext struct {
	store *stateStore
	key   string
}

// Key returns the key of the event being processed.
func (c *StateContext) Key() string {
	return c.key
}

// ValueOf returns the ValueState[T] named `name`, bound to the current key.
// (Go methods cannot be generic, so handles are package-level functions.)
func ValueOf[T any](c *StateContext, name string) ValueState[T] {
	return ValueState[T]{store: c.store, name: name, key: c.key}
}

// ListOf returns the ListState[T] named `name`, bound to the current key.
func ListOf[T any](c *StateContext, name string) ListState[T] {
	return ListState[T]{store: c.store, name: name, key: c.key}
}

// MapOf returns the MapState[K, V] named `name`, bound to the current key.
func MapOf[K comparable, V any](c *StateContext, name string) MapState[K, V] {
	return MapState[K, V]{store: c.store, name: name, key: c.key}
}

type valueCell struct {
	v   any
	set bool
}

type listCell struct {
	items []any
}

type mapCell struct {
	m map[any]any
}

// ValueState is a single typed value scoped to (state name, current key).
type ValueState[T any] struct {
	store *stateStore
	name  string
	key   string
}

// Get returns the value and whether it was ever set.
func (s ValueState[T]) Get() (T, bool) {
	cell, ok := s.store.lookup(s.name, s.key)
	if !ok {
		var zero T
		return zero, false
	}
	vc := cell.(*valueCell)
	if !vc.set {
		var zero T
		return zero, false
	}
	return vc.v.(T), true
}

// Set stores the value.
func (s ValueState[T]) Set(v T) {
	cell := s.store.getOrCreate(s.name, s.key, func() any { return &valueCell{} })
	vc := cell.(*valueCell)
	vc.v = v
	vc.set = true
}

// Clear removes the value for the current key.
func (s ValueState[T]) Clear() {
	s.store.clear(s.name, s.key)
}

// ListState is an append-only list scoped to (state name, current key).
type ListState[T any] struct {
	store *stateStore
	name  string
	key   string
}

// Get returns a copy of the list contents.
func (s ListState[T]) Get() []T {
	cell, ok := s.store.lookup(s.name, s.key)
	if !ok {
		return nil
	}
	lc := cell.(*listCell)
	out := make([]T, 0, len(lc.items))
	for _, item := range lc.items {
		out = append(out, item.(T))
	}
	return out
}

// Add appends a value.
func (s ListState[T]) Add(v T) {
	cell := s.store.getOrCreate(s.name, s.key, func() any { return &listCell{} })
	lc := cell.(*listCell)
	lc.items = append(lc.items, v)
}

// Clear removes the list for the current key.
func (s ListState[T]) Clear() {
	s.store.clear(s.name, s.key)
}

// MapState is a typed map scoped to (state name, current key).
type MapState[K comparable, V any] struct {
	store *stateStore
	name  string
	key   string
}

// Get returns the value for k and whether it exists.
func (s MapState[K, V]) Get(k K) (V, bool) {
	cell, ok := s.store.lookup(s.name, s.key)
	if !ok {
		var zero V
		return zero, false
	}
	mc := cell.(*mapCell)
	v, ok := mc.m[k]
	if !ok {
		var zero V
		return zero, false
	}
	return v.(V), true
}

// Put stores k -> v.
func (s MapState[K, V]) Put(k K, v V) {
	cell := s.store.getOrCreate(s.name, s.key, func() any { return &mapCell{m: make(map[any]any)} })
	mc := cell.(*mapCell)
	mc.m[k] = v
}

// Remove deletes k.
func (s MapState[K, V]) Remove(k K) {
	cell, ok := s.store.lookup(s.name, s.key)
	if !ok {
		return
	}
	mc := cell.(*mapCell)
	delete(mc.m, k)
}

// Len returns the number of entries for the current key.
func (s MapState[K, V]) Len() int {
	cell, ok := s.store.lookup(s.name, s.key)
	if !ok {
		return 0
	}
	return len(cell.(*mapCell).m)
}

// Clear removes the whole map for the current key.
func (s MapState[K, V]) Clear() {
	s.store.clear(s.name, s.key)
}
