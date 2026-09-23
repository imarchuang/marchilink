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
		Sink(runtime.StdoutSink{Writer: &out})

	if err := job.Run(context.Background()); err != nil {
		t.Fatalf("run job: %v", err)
	}

	if got := out.String(); !strings.Contains(got, "EVENT-0") || !strings.Contains(got, "EVENT-1") {
		t.Fatalf("unexpected output: %q", got)
	}
}
