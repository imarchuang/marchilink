package runtime

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// HTTPServer exposes minimal observability endpoints for a graph.
type HTTPServer struct {
	Graph *Graph

	// Throughput sampling state: rates are computed between successive
	// calls to /debug/throughput.
	mu         sync.Mutex
	lastAt     time.Time
	lastSource int64
	lastSubs   []int64
}

// subtaskThroughputJSON adds the computed rate to the raw counters.
type subtaskThroughputJSON struct {
	SubtaskThroughput
	RecordsPerSec float64 `json:"recordsPerSec"`
}

type throughputJSON struct {
	SourceRecords    int64                   `json:"sourceRecords"`
	SourcePerSec     float64                 `json:"sourcePerSec"`
	SampleIntervalMs int64                   `json:"sampleIntervalMs"`
	Subtasks         []subtaskThroughputJSON `json:"subtasks"`
}

// Handler returns the HTTP handler for the server.
func (s *HTTPServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("marchilink\n\nGET /healthz\nGET /jobs/demo/watermarks\nGET /jobs/demo/state\nGET /checkpoints\nGET /debug/throughput\nPOST /savepoints/{name}\n"))
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/jobs/demo/watermarks", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s.Graph.Watermarks())
	})
	mux.HandleFunc("/jobs/demo/state", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s.Graph.State())
	})
	mux.HandleFunc("/checkpoints", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s.Graph.Checkpoints())
	})
	mux.HandleFunc("POST /savepoints/{name}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		meta, err := s.Graph.TriggerSavepoint(r.Context(), r.PathValue("name"))
		if err != nil {
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(meta)
	})
	mux.HandleFunc("/debug/throughput", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s.throughput())
	})
	return mux
}

// throughput samples the raw counters and computes per-second rates against
// the previous sample. The first call after startup reports zero rates.
func (s *HTTPServer) throughput() throughputJSON {
	snap := s.Graph.Throughput()

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	out := throughputJSON{SourceRecords: snap.SourceRecords}
	if !s.lastAt.IsZero() {
		dt := now.Sub(s.lastAt).Seconds()
		if dt > 0 {
			out.SampleIntervalMs = now.Sub(s.lastAt).Milliseconds()
			out.SourcePerSec = float64(snap.SourceRecords-s.lastSource) / dt
		}
	}
	for i, sub := range snap.Subtasks {
		rate := 0.0
		if out.SampleIntervalMs > 0 && i < len(s.lastSubs) {
			rate = float64(sub.Processed-s.lastSubs[i]) / (float64(out.SampleIntervalMs) / 1000)
		}
		out.Subtasks = append(out.Subtasks, subtaskThroughputJSON{SubtaskThroughput: sub, RecordsPerSec: rate})
	}

	s.lastAt = now
	s.lastSource = snap.SourceRecords
	s.lastSubs = s.lastSubs[:0]
	for _, sub := range snap.Subtasks {
		s.lastSubs = append(s.lastSubs, sub.Processed)
	}
	return out
}
