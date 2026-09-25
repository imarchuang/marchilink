package runtime

import (
	"context"
	"time"
)

// ScriptedSource emits a fixed list of events in order. It tracks how many
// events have been emitted (its offset) and supports Seek for recovery.
// An optional Interval paces emission so checkpoints have time to trigger.
type ScriptedSource struct {
	Events   []Event
	Interval time.Duration

	offset int64
}

// Run implements Source.
func (s *ScriptedSource) Run(ctx context.Context, emit func(Event) error) error {
	for ; s.offset < int64(len(s.Events)); s.offset++ {
		if err := emit(s.Events[s.offset]); err != nil {
			return err
		}
		if s.Interval > 0 && s.offset < int64(len(s.Events))-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(s.Interval):
			}
		}
	}
	return nil
}

// Offset returns the number of events already emitted.
func (s *ScriptedSource) Offset() int64 {
	return s.offset
}

// Seek rewinds the source to a previously checkpointed offset.
func (s *ScriptedSource) Seek(offset int64) {
	if offset < 0 {
		offset = 0
	}
	if offset > int64(len(s.Events)) {
		offset = int64(len(s.Events))
	}
	s.offset = offset
}
