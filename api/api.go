package api

import (
	"context"
	"strings"
	"time"

	"github.com/imarchuang/marchilink/runtime"
)

// Job is the user-facing job definition.
type Job struct {
	name   string
	source runtime.Source
	mapFn  runtime.MapFunc
	keyBy  runtime.KeyFunc
	sink   runtime.Sink

	assigner runtime.WindowAssigner
	agg      runtime.AggFactory

	parallelism     int
	channelCapacity int
	watermarkBound  time.Duration
}

// NewJob creates a named job.
func NewJob(name string) *Job {
	return &Job{name: name, parallelism: 1, channelCapacity: 16, watermarkBound: 5 * time.Second}
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

// WatermarkBound sets bounded out-of-orderness for event time.
func (j *Job) WatermarkBound(bound time.Duration) *Job {
	if bound >= 0 {
		j.watermarkBound = bound
	}
	return j
}

// Window sets the event-time window assigner and aggregator.
func (j *Job) Window(assigner runtime.WindowAssigner, agg runtime.AggFactory) *Job {
	j.assigner = assigner
	j.agg = agg
	return j
}

// Graph builds the runtime graph for the job.
func (j *Job) Graph() *runtime.Graph {
	graph := runtime.NewGraph(j.source, j.mapFn, j.keyBy, j.sink)
	graph.SetParallelism(j.parallelism)
	graph.SetChannelCapacity(j.channelCapacity)
	graph.SetWatermarkBound(j.watermarkBound)
	if j.assigner != nil {
		graph.SetWindow(j.assigner, j.agg)
	}
	return graph
}

// Run executes the job graph.
func (j *Job) Run(ctx context.Context) error {
	return j.Graph().Run(ctx)
}

// UppercaseValue is a tiny demo mapper for slice 0.
func UppercaseValue(event runtime.Event) runtime.Event {
	event.Value = strings.ToUpper(event.Value)
	return event
}
