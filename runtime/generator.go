package runtime

import (
	"context"
	"fmt"
	"time"
)

// GeneratorSource emits a fixed number of synthetic events.
type GeneratorSource struct {
	Count    int
	Interval time.Duration
}

// Run implements Source.
func (s GeneratorSource) Run(ctx context.Context, emit func(Event) error) error {
	if s.Count <= 0 {
		return nil
	}

	interval := s.Interval
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for i := 0; i < s.Count; i++ {
		event := Event{
			Key:       fmt.Sprintf("key-%d", i%4),
			Value:     fmt.Sprintf("event-%d", i),
			Timestamp: time.Now().UTC(),
		}

		if err := emit(event); err != nil {
			return err
		}

		if i == s.Count-1 {
			break
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}

	return nil
}
