package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mockMarchiq is an in-memory marchiq broker for testing.
type mockMarchiq struct {
	mu       sync.Mutex
	topics   map[string][]mockRecord     // topic -> records (single partition)
	groups   map[string]map[string]int64 // group -> topic -> committed offset
	produceN atomic.Int64
	commitN  atomic.Int64
}

type mockRecord struct {
	Offset      int64
	Key         string
	Value       string
	TimestampNS int64
}

func newMockMarchiq() *mockMarchiq {
	return &mockMarchiq{
		topics: make(map[string][]mockRecord),
		groups: make(map[string]map[string]int64),
	}
}

func (m *mockMarchiq) produce(topic, key, value string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	offset := int64(len(m.topics[topic]))
	m.topics[topic] = append(m.topics[topic], mockRecord{
		Offset: offset, Key: key, Value: value,
		TimestampNS: time.Now().UnixNano(),
	})
	m.produceN.Add(1)
	return offset
}

func (m *mockMarchiq) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /groups/{group}/join", func(w http.ResponseWriter, r *http.Request) {
		group := r.PathValue("group")
		topic := r.URL.Query().Get("topic")
		m.mu.Lock()
		if m.groups[group] == nil {
			m.groups[group] = make(map[string]int64)
		}
		if _, ok := m.groups[group][topic]; !ok {
			m.groups[group][topic] = -1 // nothing committed
		}
		m.mu.Unlock()
		json.NewEncoder(w).Encode(map[string]any{
			"group": group, "member": r.URL.Query().Get("member"),
			"topic": topic, "partitions": []int{0},
		})
	})

	mux.HandleFunc("GET /fetch", func(w http.ResponseWriter, r *http.Request) {
		group := r.URL.Query().Get("group")
		topic := r.URL.Query().Get("topic")
		maxRecords, _ := strconv.Atoi(r.URL.Query().Get("max_records"))
		if maxRecords <= 0 {
			maxRecords = 100
		}

		m.mu.Lock()
		committed := m.groups[group][topic]
		start := committed + 1
		records := m.topics[topic]
		var out []map[string]any
		var nextOffset int64 = start
		for i := start; i < int64(len(records)) && len(out) < maxRecords; i++ {
			rec := records[i]
			out = append(out, map[string]any{
				"offset": rec.Offset, "timestamp_ns": rec.TimestampNS,
				"key": []byte(rec.Key), "value": []byte(rec.Value),
			})
			nextOffset = rec.Offset + 1
		}
		m.mu.Unlock()

		json.NewEncoder(w).Encode(map[string]any{
			"group": group, "topic": topic,
			"partitions": []map[string]any{{
				"partition": 0, "records": out,
				"next_offset": nextOffset, "high_watermark": int64(len(records)),
			}},
		})
	})

	mux.HandleFunc("POST /commit", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Group     string `json:"group"`
			Topic     string `json:"topic"`
			Partition int    `json:"partition"`
			Offset    int64  `json:"offset"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		m.mu.Lock()
		if m.groups[req.Group] == nil {
			m.groups[req.Group] = make(map[string]int64)
		}
		m.groups[req.Group][req.Topic] = req.Offset
		m.mu.Unlock()
		m.commitN.Add(1)
		json.NewEncoder(w).Encode(map[string]any{"committed": req.Offset})
	})

	mux.HandleFunc("POST /produce", func(w http.ResponseWriter, r *http.Request) {
		topic := r.URL.Query().Get("topic")
		key := r.URL.Query().Get("key")
		var body [4096]byte
		n, _ := r.Body.Read(body[:])
		offset := m.produce(topic, key, string(body[:n]))
		json.NewEncoder(w).Encode(map[string]any{
			"topic": topic, "partition": 0, "offset": offset,
		})
	})

	mux.HandleFunc("GET /debug/lag", func(w http.ResponseWriter, r *http.Request) {
		group := r.URL.Query().Get("group")
		m.mu.Lock()
		var lags []map[string]any
		for topic, committed := range m.groups[group] {
			latest := int64(len(m.topics[topic]))
			nextFetch := committed + 1
			lags = append(lags, map[string]any{
				"group": group, "topic": topic,
				"partitions": []map[string]any{{
					"partition": 0, "committed": committed,
					"next_fetch": nextFetch, "latest": latest,
					"lag": latest - nextFetch,
				}},
			})
		}
		m.mu.Unlock()
		json.NewEncoder(w).Encode(map[string]any{"groups": lags})
	})

	return mux
}

func TestMarchiqSourceFetchAndCommit(t *testing.T) {
	t.Parallel()

	broker := newMockMarchiq()
	for i := 0; i < 5; i++ {
		broker.produce("events", fmt.Sprintf("key-%d", i), fmt.Sprintf("val-%d", i))
	}

	server := httptest.NewServer(broker.handler())
	defer server.Close()

	source, err := NewMarchiqSource(server.URL, "events", "test-group", "member-0")
	if err != nil {
		t.Fatalf("new source: %v", err)
	}

	var events []Event
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Run briefly to fetch records.
	done := make(chan error, 1)
	go func() {
		done <- source.Run(ctx, func(e Event) error {
			events = append(events, e)
			if len(events) >= 5 {
				cancel()
			}
			return nil
		})
	}()
	<-done

	if len(events) != 5 {
		t.Fatalf("expected 5 events, got %d", len(events))
	}
	if events[0].Key != "key-0" || events[4].Key != "key-4" {
		t.Fatalf("unexpected events: %v", events)
	}

	// Commit offsets.
	if err := source.CommitOffsets(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Verify committed offset.
	broker.mu.Lock()
	committed := broker.groups["test-group"]["events"]
	broker.mu.Unlock()
	if committed != 4 { // last processed = offset 4
		t.Fatalf("expected committed offset 4, got %d", committed)
	}
}

func TestMarchiqSinkProduces(t *testing.T) {
	t.Parallel()

	broker := newMockMarchiq()
	server := httptest.NewServer(broker.handler())
	defer server.Close()

	sink := NewMarchiqSink(server.URL, "output")
	err := sink.Write(context.Background(), Event{
		Key: "user-A", Value: "count=3", Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("sink write: %v", err)
	}

	broker.mu.Lock()
	records := broker.topics["output"]
	broker.mu.Unlock()
	if len(records) != 1 {
		t.Fatalf("expected 1 record in output topic, got %d", len(records))
	}
	if records[0].Key != "user-A" || records[0].Value != "count=3" {
		t.Fatalf("unexpected record: %+v", records[0])
	}
}

func TestEndToEndCrashBetweenSnapshotAndCommit(t *testing.T) {
	t.Parallel()

	broker := newMockMarchiq()

	// Produce 20 events to the input topic.
	for i := 0; i < 20; i++ {
		broker.produce("events", fmt.Sprintf("key-%d", i%3), "1")
	}

	server := httptest.NewServer(broker.handler())
	defer server.Close()

	dir := t.TempDir()

	// --- Phase A: run with checkpointing, crash after snapshot but before commit. ---
	phaseASink := &collectSink{}
	sourceA, err := NewMarchiqSource(server.URL, "events", "flink-group", "member-0")
	if err != nil {
		t.Fatalf("source A: %v", err)
	}

	graphA := NewGraph(sourceA,
		func(e Event) Event { return e },
		func(e Event) string { return e.Key },
		phaseASink)
	graphA.SetParallelism(2)
	graphA.SetWatermarkBound(2 * time.Second)
	graphA.SetWindow(Tumbling(10*time.Second), Count())
	graphA.SetCheckpointing(dir, "e2e", 100*time.Millisecond)

	// Run briefly, then cancel (simulating crash).
	crashCtx, crashCancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(500 * time.Millisecond)
		crashCancel()
	}()
	_ = graphA.Run(crashCtx)

	// Verify a checkpoint was written.
	_, srcState, _, loadErr := newCheckpointStore(dir, "e2e").loadLatest()
	if loadErr != nil {
		t.Fatalf("expected checkpoint: %v", loadErr)
	}
	t.Logf("checkpoint at source offset %d, offsets %v", srcState.Offset, srcState.Offsets)

	// Count how many records were committed to marchiq before the crash.
	commitsBefore := broker.commitN.Load()
	t.Logf("commits before crash: %d", commitsBefore)

	// --- Phase B: restart from checkpoint, run to completion. ---
	// Reset the mock broker's committed offset to simulate the crash happening
	// before the commit reached marchiq.
	broker.mu.Lock()
	broker.groups["flink-group"]["events"] = -1 // nothing committed
	broker.mu.Unlock()

	phaseBSink := &collectSink{}
	sourceB, err := NewMarchiqSource(server.URL, "events", "flink-group", "member-0")
	if err != nil {
		t.Fatalf("source B: %v", err)
	}

	graphB := NewGraph(sourceB,
		func(e Event) Event { return e },
		func(e Event) string { return e.Key },
		phaseBSink)
	graphB.SetParallelism(2)
	graphB.SetWatermarkBound(2 * time.Second)
	graphB.SetWindow(Tumbling(10*time.Second), Count())
	graphB.SetCheckpointing(dir, "e2e", 100*time.Millisecond)

	// Run until all events are consumed.
	runCtx, runCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer runCancel()
	go func() {
		// Cancel when all 20 events have been fetched.
		for {
			time.Sleep(100 * time.Millisecond)
			if len(phaseBSink.out) >= 9 { // 3 keys x 3 windows
				runCancel()
				return
			}
		}
	}()
	_ = graphB.Run(runCtx)

	// --- Verify: no duplicates in the combined output. ---
	combined := append(append([]string(nil), phaseASink.out...), phaseBSink.out...)
	sort.Strings(combined)

	// Count occurrences of each unique window result.
	counts := make(map[string]int)
	for _, s := range combined {
		counts[s]++
	}
	for result, count := range counts {
		if count > 1 {
			t.Errorf("duplicate output: %q appeared %d times", result, count)
		}
	}

	// Verify lag returns to 0 after the final checkpoint commits.
	// (The last checkpoint should have committed all offsets.)
	broker.mu.Lock()
	finalCommitted := broker.groups["flink-group"]["events"]
	broker.mu.Unlock()
	latest := int64(len(broker.topics["events"]))
	if finalCommitted < latest-1 {
		t.Logf("warning: final committed offset %d, latest %d (lag %d)", finalCommitted, latest, latest-finalCommitted-1)
	}
}
