package runtime

import "context"

// Sink consumes events from the job graph.
type Sink interface {
	Write(ctx context.Context, event Event) error
}

// SinkFunc adapts a function into a Sink.
type SinkFunc func(ctx context.Context, event Event) error

// Write implements Sink.
func (f SinkFunc) Write(ctx context.Context, event Event) error {
	return f(ctx, event)
}
