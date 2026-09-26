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
		jobName     = flag.String("job", "windowed-count", "demo job: windowed-count | dedup | running-count")
		count       = flag.Int("count", 10, "number of events to generate")
		interval    = flag.Duration("interval", 100*time.Millisecond, "delay between generated events")
		parallelism = flag.Int("parallelism", 2, "number of downstream subtasks")
		buffer      = flag.Int("buffer", 4, "bounded channel capacity per edge")
		wmBound     = flag.Duration("watermark-bound", 2*time.Second, "bounded out-of-orderness for watermarks")
		windowSize  = flag.Duration("window", 5*time.Second, "window size")
		windowSlide = flag.Duration("slide", 0, "window slide (0 = tumbling)")
		httpAddr    = flag.String("http", ":9081", "HTTP observability address")
		dataDir     = flag.String("data-dir", "", "state backend directory (empty = no checkpoints/savepoints)")
		chkEvery    = flag.Duration("checkpoint", 0, "checkpoint interval (0 = savepoints only)")
		resumeFrom  = flag.String("resume", "", "resume from named savepoint under {data-dir}/savepoints")
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
		event := runtime.Event{
			Key:       fmt.Sprintf("key-%d", i%4),
			Value:     fmt.Sprintf("event-%d", i),
			Timestamp: ts,
		}
		if *jobName == "dedup" && i%7 == 6 && i > 0 {
			// Inject duplicates: resend the previous (key, order id) pair.
			event.Key = events[i-1].Key
			event.Value = events[i-1].Value
		}
		events = append(events, event)
	}

	job := api.NewJob(*jobName).
		Source(&runtime.ScriptedSource{Events: events}).
		Map(api.UppercaseValue).
		KeyBy(func(event runtime.Event) string { return event.Key }).
		Parallelism(*parallelism).
		ChannelCapacity(*buffer).
		WatermarkBound(*wmBound).
		Sink(runtime.StdoutSink{Writer: os.Stdout})

	if *dataDir != "" {
		job.Checkpointing(*dataDir, *chkEvery)
	}
	if *resumeFrom != "" {
		job.ResumeFrom(*resumeFrom)
	}

	switch *jobName {
	case "dedup":
		job.Process(dedup)
	case "running-count":
		job.Process(runningCount)
	default: // windowed-count
		var assigner runtime.WindowAssigner
		if *windowSlide > 0 {
			assigner = runtime.Sliding(*windowSize, *windowSlide)
		} else {
			assigner = runtime.Tumbling(*windowSize)
		}
		job.Window(assigner, runtime.Count())
	}

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

// dedup filters events whose (key, order id) was already seen, using MapState.
func dedup(ctx *runtime.StateContext, event runtime.Event) ([]runtime.Event, error) {
	seen := runtime.MapOf[string, bool](ctx, "seen-orders")
	if _, ok := seen.Get(event.Value); ok {
		return nil, nil
	}
	seen.Put(event.Value, true)
	return []runtime.Event{event}, nil
}

// runningCount maintains a per-key counter in ValueState and annotates events.
func runningCount(ctx *runtime.StateContext, event runtime.Event) ([]runtime.Event, error) {
	count := runtime.ValueOf[int64](ctx, "running-count")
	n, _ := count.Get()
	n++
	count.Set(n)
	event.Value = fmt.Sprintf("%s count=%d", event.Value, n)
	return []runtime.Event{event}, nil
}
