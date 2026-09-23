package runtime

import "context"

// MapFunc transforms one event into another.
type MapFunc func(Event) Event

// Pipeline is the slice-0 linear processing loop.
type Pipeline struct {
	source Source
	mapFn  MapFunc
	sink   Sink
}

// NewPipeline builds a minimal source -> map -> sink pipeline.
func NewPipeline(source Source, mapFn MapFunc, sink Sink) *Pipeline {
	return &Pipeline{source: source, mapFn: mapFn, sink: sink}
}

// Run starts the pipeline and blocks until the source stops or fails.
func (p *Pipeline) Run(ctx context.Context) error {
	return p.source.Run(ctx, func(event Event) error {
		return p.sink.Write(ctx, p.mapFn(event))
	})
}
