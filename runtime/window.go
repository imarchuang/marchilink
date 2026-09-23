package runtime

import (
	"strconv"
	"time"
)

// Window is a half-open event-time interval [Start, End).
type Window struct {
	Start time.Time
	End   time.Time
}

// WindowAssigner assigns an event timestamp to one or more windows.
type WindowAssigner interface {
	Assign(ts time.Time) []Window
}

// Tumbling returns a tumbling window assigner: fixed-size, non-overlapping.
func Tumbling(size time.Duration) WindowAssigner {
	return tumblingAssigner{size: size}
}

type tumblingAssigner struct {
	size time.Duration
}

func (a tumblingAssigner) Assign(ts time.Time) []Window {
	size := int64(a.size)
	if size <= 0 {
		return nil
	}
	start := ts.UnixNano() / size * size
	return []Window{{
		Start: time.Unix(0, start).UTC(),
		End:   time.Unix(0, start+size).UTC(),
	}}
}

// Sliding returns a sliding window assigner: fixed-size, overlapping by slide.
func Sliding(size, slide time.Duration) WindowAssigner {
	return slidingAssigner{size: size, slide: slide}
}

type slidingAssigner struct {
	size  time.Duration
	slide time.Duration
}

func (a slidingAssigner) Assign(ts time.Time) []Window {
	size := int64(a.size)
	slide := int64(a.slide)
	if size <= 0 || slide <= 0 {
		return nil
	}

	n := ts.UnixNano()
	lastStart := n / slide * slide

	var windows []Window
	for start := lastStart; start+size > n; start -= slide {
		windows = append(windows, Window{
			Start: time.Unix(0, start).UTC(),
			End:   time.Unix(0, start+size).UTC(),
		})
	}

	// Return windows in ascending start order.
	for i, j := 0, len(windows)-1; i < j; i, j = i+1, j-1 {
		windows[i], windows[j] = windows[j], windows[i]
	}
	return windows
}

// Aggregator accumulates events inside one (key, window) state cell.
type Aggregator interface {
	Add(event Event)
	Result() string
}

// AggFactory creates a fresh Aggregator per (key, window).
type AggFactory func() Aggregator

// Count counts events per window.
func Count() AggFactory {
	return func() Aggregator { return &countAggregator{} }
}

type countAggregator struct {
	n int64
}

func (a *countAggregator) Add(Event) { a.n++ }

func (a *countAggregator) Result() string { return strconv.FormatInt(a.n, 10) }

// Sum sums events whose Value parses as a float; others add 0.
func Sum() AggFactory {
	return func() Aggregator { return &sumAggregator{} }
}

type sumAggregator struct {
	v float64
}

func (a *sumAggregator) Add(event Event) {
	f, err := strconv.ParseFloat(event.Value, 64)
	if err == nil {
		a.v += f
	}
}

func (a *sumAggregator) Result() string {
	return strconv.FormatFloat(a.v, 'f', -1, 64)
}
