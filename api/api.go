package api

import (
	"context"
	"strings"

	"github.com/imarchuang/marchilink/runtime"
)

// Job is the user-facing slice-0 job definition.
type Job struct {
	name   string
	source runtime.Source
	mapFn  runtime.MapFunc
	sink   runtime.Sink
}

// NewJob creates a named job.
func NewJob(name string) *Job {
	return &Job{name: name}
}

// Name returns the job name.
func (j *Job) Name() string {
	return j.name
}

// Source sets the job source.
func (j *Job) Source(source runtime.Source) *Job {
	j.source = source
	return j
}

// Map sets the job mapper.
func (j *Job) Map(mapFn runtime.MapFunc) *Job {
	j.mapFn = mapFn
	return j
}

// Sink sets the job sink.
func (j *Job) Sink(sink runtime.Sink) *Job {
	j.sink = sink
	return j
}

// Run executes the job pipeline.
func (j *Job) Run(ctx context.Context) error {
	pipeline := runtime.NewPipeline(j.source, j.mapFn, j.sink)
	return pipeline.Run(ctx)
}

// UppercaseValue is a tiny demo mapper for slice 0.
func UppercaseValue(event runtime.Event) runtime.Event {
	event.Value = strings.ToUpper(event.Value)
	return event
}
