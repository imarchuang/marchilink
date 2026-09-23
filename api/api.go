package api

import (
	"context"
	"strings"

	"github.com/imarchuang/marchilink/runtime"
)

// Job is the user-facing job definition.
type Job struct {
	name   string
	source runtime.Source
	mapFn  runtime.MapFunc
	keyBy  runtime.KeyFunc
	sink   runtime.Sink

	parallelism     int
	channelCapacity int
}

// NewJob creates a named job.
func NewJob(name string) *Job {
	return &Job{name: name, parallelism: 1, channelCapacity: 16}
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

// KeyBy sets the key extractor for downstream partitioning.
func (j *Job) KeyBy(keyBy runtime.KeyFunc) *Job {
	j.keyBy = keyBy
	return j
}

// Sink sets the job sink.
func (j *Job) Sink(sink runtime.Sink) *Job {
	j.sink = sink
	return j
}

// Parallelism sets the number of downstream subtasks.
func (j *Job) Parallelism(parallelism int) *Job {
	if parallelism > 0 {
		j.parallelism = parallelism
	}
	return j
}

// ChannelCapacity sets the bounded channel size per edge.
func (j *Job) ChannelCapacity(capacity int) *Job {
	if capacity > 0 {
		j.channelCapacity = capacity
	}
	return j
}

// Run executes the job graph.
func (j *Job) Run(ctx context.Context) error {
	graph := runtime.NewGraph(j.source, j.mapFn, j.keyBy, j.sink)
	graph.SetParallelism(j.parallelism)
	graph.SetChannelCapacity(j.channelCapacity)
	return graph.Run(ctx)
}

// UppercaseValue is a tiny demo mapper for slice 0.
func UppercaseValue(event runtime.Event) runtime.Event {
	event.Value = strings.ToUpper(event.Value)
	return event
}
