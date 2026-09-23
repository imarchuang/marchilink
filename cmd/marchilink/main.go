package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/imarchuang/marchilink/api"
	"github.com/imarchuang/marchilink/runtime"
)

func main() {
	var (
		count    = flag.Int("count", 10, "number of events to generate")
		interval = flag.Duration("interval", 100*time.Millisecond, "delay between generated events")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	job := api.NewJob("slice0-generator").
		Source(runtime.GeneratorSource{Count: *count, Interval: *interval}).
		Map(api.UppercaseValue).
		Sink(runtime.StdoutSink{Writer: os.Stdout})

	if err := job.Run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "marchilink:", err)
		os.Exit(1)
	}
}
