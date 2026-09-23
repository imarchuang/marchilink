package runtime

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

func TestPipelineRunsSourceMapSink(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	pipeline := NewPipeline(
		GeneratorSource{Count: 3, Interval: time.Millisecond},
		func(event Event) Event {
			event.Value = strings.ToUpper(event.Value)
			return event
		},
		StdoutSink{Writer: &out},
	)

	if err := pipeline.Run(context.Background()); err != nil {
		t.Fatalf("run pipeline: %v", err)
	}

	got := out.String()
	for _, want := range []string{"EVENT-0", "EVENT-1", "EVENT-2"} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected output to contain %q, got %q", want, got)
		}
	}
}
