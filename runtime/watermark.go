package runtime

import (
	"sync"
	"time"
)

// WatermarkTracker tracks watermarks for a single input and an operator view.
type WatermarkTracker struct {
	mu      sync.RWMutex
	current time.Time
	inputs  []time.Time
	ready   []bool
	bound   time.Duration
	seenMax time.Time
	started bool
}

// NewWatermarkTracker creates a tracker for one operator.
func NewWatermarkTracker(inputCount int, bound time.Duration) *WatermarkTracker {
	if inputCount <= 0 {
		inputCount = 1
	}
	return &WatermarkTracker{
		inputs: make([]time.Time, inputCount),
		ready:  make([]bool, inputCount),
		bound:  bound,
	}
}

// ObserveEvent records an event timestamp and returns the source watermark.
func (t *WatermarkTracker) ObserveEvent(ts time.Time) time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.started || ts.After(t.seenMax) {
		t.seenMax = ts
	}
	t.started = true

	wm := t.seenMax.Add(-t.bound)
	if wm.After(t.current) {
		t.current = wm
	}
	return t.current
}

// UpdateInput records a watermark from one input and returns the operator watermark.
func (t *WatermarkTracker) UpdateInput(idx int, wm time.Time) time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()

	if idx >= 0 && idx < len(t.inputs) {
		t.inputs[idx] = wm
		t.ready[idx] = true
	}

	started := false
	var min time.Time
	for i, input := range t.inputs {
		if !t.ready[i] {
			continue
		}
		if !started || input.Before(min) {
			min = input
			started = true
		}
	}

	if started && (!t.started || min.After(t.current)) {
		t.current = min
		t.started = true
	}
	return t.current
}

// Current returns the latest watermark.
func (t *WatermarkTracker) Current() time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.current
}
