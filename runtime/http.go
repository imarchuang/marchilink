package runtime

import (
	"encoding/json"
	"net/http"
)

// HTTPServer exposes minimal observability endpoints for a graph.
type HTTPServer struct {
	Graph *Graph
}

// Handler returns the HTTP handler for the server.
func (s HTTPServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("marchilink\n\nGET /healthz\nGET /jobs/demo/watermarks\nGET /jobs/demo/state\nGET /checkpoints\nPOST /savepoints/{name}\n"))
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
	return mux
}
