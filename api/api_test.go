package api

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/imarchuang/marchilink/runtime"
)

func TestJobRun(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	job := NewJob("test-job").
		Source(runtime.GeneratorSource{Count: 2, Interval: time.Millisecond}).
		Map(UppercaseValue).
		KeyBy(func(event runtime.Event) string { return event.Key }).
		Sink(runtime.StdoutSink{Writer: &out})

	if err := job.Run(context.Background()); err != nil {
		t.Fatalf("run job: %v", err)
	}

	if got := out.String(); !strings.Contains(got, "EVENT-0") || !strings.Contains(got, "EVENT-1") {
		t.Fatalf("unexpected output: %q", got)
	}
}

func TestJobRunWindowed(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	var out bytes.Buffer
	job := NewJob("windowed-job").
		Source(runtime.ScriptedSource{Events: []runtime.Event{
			{Key: "a", Value: "1", Timestamp: base.Add(1 * time.Second)},
			{Key: "a", Value: "2", Timestamp: base.Add(3 * time.Second)},
		}}).
		Map(UppercaseValue).
		KeyBy(func(event runtime.Event) string { return event.Key }).
		Window(runtime.Tumbling(10*time.Second), runtime.Count()).
		Sink(runtime.StdoutSink{Writer: &out})

	if err := job.Run(context.Background()); err != nil {
		t.Fatalf("run job: %v", err)
	}

	got := out.String()
	if !strings.Contains(got, "window[") || !strings.Contains(got, " 2") {
		t.Fatalf("expected a fired window with count 2, got %q", got)
	}
}

func TestJobGraphExposesWatermarks(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	job := NewJob("wm-job").
		Source(runtime.ScriptedSource{Events: []runtime.Event{
			{Key: "a", Value: "1", Timestamp: base.Add(3 * time.Second)},
			{Key: "a", Value: "2", Timestamp: base.Add(5 * time.Second)},
		}}).
		Map(UppercaseValue).
		KeyBy(func(event runtime.Event) string { return event.Key }).
		WatermarkBound(time.Second).
		Sink(runtime.StdoutSink{Writer: &bytes.Buffer{}})

	graph := job.Graph()
	if err := graph.Run(context.Background()); err != nil {
		t.Fatalf("run graph: %v", err)
	}

	if got := graph.Watermarks().Source; !got.Equal(base.Add(4 * time.Second)) {
		t.Fatalf("unexpected source watermark: %s", got)
	}
}
