package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/imarchuang/marchilink/api"
	"github.com/imarchuang/marchilink/runtime"
)

func main() {
	var (
		count       = flag.Int("count", 10, "number of events to generate")
		interval    = flag.Duration("interval", 100*time.Millisecond, "delay between generated events")
		parallelism = flag.Int("parallelism", 2, "number of downstream subtasks")
		buffer      = flag.Int("buffer", 4, "bounded channel capacity per edge")
		wmBound     = flag.Duration("watermark-bound", 2*time.Second, "bounded out-of-orderness for watermarks")
		windowSize  = flag.Duration("window", 5*time.Second, "window size")
		windowSlide = flag.Duration("slide", 0, "window slide (0 = tumbling)")
		httpAddr    = flag.String("http", ":9081", "HTTP observability address")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	base := time.Now().UTC()
	events := make([]runtime.Event, 0, *count)
	for i := 0; i < *count; i++ {
		ts := base.Add(time.Duration(i) * (*interval))
		if i%5 == 4 {
			ts = ts.Add(-(*interval))
		}
		events = append(events, runtime.Event{
			Key:       fmt.Sprintf("key-%d", i%4),
			Value:     fmt.Sprintf("event-%d", i),
			Timestamp: ts,
		})
	}

	var assigner runtime.WindowAssigner
	if *windowSlide > 0 {
		assigner = runtime.Sliding(*windowSize, *windowSlide)
	} else {
		assigner = runtime.Tumbling(*windowSize)
	}

	job := api.NewJob("slice3-windows").
		Source(runtime.ScriptedSource{Events: events}).
		Map(api.UppercaseValue).
		KeyBy(func(event runtime.Event) string { return event.Key }).
		Window(assigner, runtime.Count()).
		Parallelism(*parallelism).
		ChannelCapacity(*buffer).
		WatermarkBound(*wmBound).
		Sink(runtime.StdoutSink{Writer: os.Stdout})

	graph := job.Graph()
	server := &http.Server{Addr: *httpAddr, Handler: runtime.HTTPServer{Graph: graph}.Handler()}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Fprintln(os.Stderr, "http:", err)
		}
	}()

	if err := graph.Run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "marchilink:", err)
		os.Exit(1)
	}
}
