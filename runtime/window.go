package runtime

import (
	"fmt"
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

// Session returns a session window assigner: each event opens a window
// [ts, ts+gap), and windows that touch or overlap merge into one. A session
// closes when no event arrives for gap — i.e. it fires when the watermark
// passes last-event-ts + gap.
func Session(gap time.Duration) WindowAssigner {
	return sessionAssigner{gap: gap}
}

type sessionAssigner struct {
	gap time.Duration
}

func (a sessionAssigner) Assign(ts time.Time) []Window {
	if a.gap <= 0 {
		return nil
	}
	return []Window{{Start: ts.UTC(), End: ts.Add(a.gap).UTC()}}
}

// Merges marks session windows as merging: the window operator unions
// overlapping windows per key instead of keeping them separate.
func (a sessionAssigner) Merges() bool { return true }

// mergingAssigner is implemented by assigners whose windows merge on contact
// (session windows). The window operator type-asserts for it.
type mergingAssigner interface {
	Merges() bool
}

// windowsMerge reports whether two windows merge into one session. Like
// Flink's TimeWindow.intersects the comparison is inclusive: an event
// arriving exactly gap after another still extends the session.
func windowsMerge(a, b Window) bool {
	return !a.Start.After(b.End) && !b.Start.After(a.End)
}

// Aggregator accumulates events inside one (key, window) state cell.
type Aggregator interface {
	Add(event Event)
	Result() string
	// snapshotKind identifies the aggregator type for checkpoint restore.
	snapshotKind() string
	// restore sets the aggregator's value from a checkpointed Result string.
	restore(value string) error
	// merge folds another aggregator of the same kind into this one.
	// Needed by merging (session) windows.
	merge(other Aggregator) error
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

func (a *countAggregator) snapshotKind() string { return "count" }

func (a *countAggregator) restore(value string) error {
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return err
	}
	a.n = n
	return nil
}

func (a *countAggregator) merge(other Aggregator) error {
	o, ok := other.(*countAggregator)
	if !ok {
		return fmt.Errorf("cannot merge %T into countAggregator", other)
	}
	a.n += o.n
	return nil
}

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

func (a *sumAggregator) snapshotKind() string { return "sum" }

func (a *sumAggregator) restore(value string) error {
	f, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return err
	}
	a.v = f
	return nil
}

func (a *sumAggregator) merge(other Aggregator) error {
	o, ok := other.(*sumAggregator)
	if !ok {
		return fmt.Errorf("cannot merge %T into sumAggregator", other)
	}
	a.v += o.v
	return nil
}
