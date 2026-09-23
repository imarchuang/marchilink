package runtime

import "context"

// Source produces events into the job graph.
type Source interface {
	Run(ctx context.Context, emit func(Event) error) error
}

// SourceFunc adapts a function into a Source.
type SourceFunc func(ctx context.Context, emit func(Event) error) error

// Run implements Source.
func (f SourceFunc) Run(ctx context.Context, emit func(Event) error) error {
	return f(ctx, emit)
}
