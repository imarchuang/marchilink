package runtime

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

// checkpointID identifies one checkpoint attempt.
type checkpointID int64

// checkpointMeta is the _metadata.json for a completed checkpoint.
type checkpointMeta struct {
	ID          checkpointID `json:"id"`
	JobID       string       `json:"jobId"`
	StartedAt   time.Time    `json:"startedAt"`
	FinishedAt  time.Time    `json:"finishedAt"`
	Parallelism int          `json:"parallelism"`
	Status      string       `json:"status"` // "completed"
}

// sourceState is the source operator's snapshot: how many records have been
// fully processed (i.e. the next record index to read).
type sourceState struct {
	Offset int64 `json:"offset"`
	// Offsets is the per-partition fetch position for marchiq sources.
	Offsets map[int]int64 `json:"offsets,omitempty"`
}

// subtaskSnapshot is one window/process subtask's full state.
type subtaskSnapshot struct {
	Clock        time.Time                      `json:"clock"`
	ProcessState map[string]map[string]any      `json:"processState"` // stateName -> key -> cell
	WindowState  map[string]map[string]aggState `json:"windowState"`  // key -> windowKey -> value
	Late         int64                          `json:"late"`
	Pending      int64                          `json:"pending"`
}

// aggState is a serialized window cell. Kind selects the aggregator type.
type aggState struct {
	Kind  string `json:"kind"`  // "count" or "sum"
	Value string `json:"value"` // serialized result
}

// windowKey encodes a Window as a map key.
func windowKey(w Window) string {
	return fmt.Sprintf("%d/%d", w.Start.UnixNano(), w.End.UnixNano())
}

func parseWindowKey(s string) (Window, error) {
	var start, end int64
	if _, err := fmt.Sscanf(s, "%d/%d", &start, &end); err != nil {
		return Window{}, err
	}
	return Window{Start: time.Unix(0, start).UTC(), End: time.Unix(0, end).UTC()}, nil
}

// checkpointStore persists checkpoints under {dataDir}/checkpoints/{jobId}.
type checkpointStore struct {
	dir string
}

func newCheckpointStore(dataDir, jobID string) *checkpointStore {
	return &checkpointStore{dir: filepath.Join(dataDir, "checkpoints", jobID)}
}

func (s *checkpointStore) chkDir(id checkpointID) string {
	return filepath.Join(s.dir, fmt.Sprintf("chk-%06d", id))
}

// write atomically publishes one checkpoint: tmp -> sync -> rename -> sync dir.
func (s *checkpointStore) write(meta checkpointMeta, src sourceState, subs []subtaskSnapshot) error {
	final := s.chkDir(meta.ID)
	tmp := final + ".tmp"

	if err := os.MkdirAll(tmp, 0o755); err != nil {
		return err
	}
	defer os.RemoveAll(tmp)

	writeJSON := func(name string, v any) error {
		f, err := os.Create(filepath.Join(tmp, name))
		if err != nil {
			return err
		}
		defer f.Close()
		if err := json.NewEncoder(f).Encode(v); err != nil {
			return err
		}
		return f.Sync()
	}

	if err := writeJSON("_metadata.json", meta); err != nil {
		return err
	}
	if err := writeJSON("source-0.state", src); err != nil {
		return err
	}
	for i, snap := range subs {
		if err := writeJSON(fmt.Sprintf("subtask-%d.state", i), snap); err != nil {
			return err
		}
	}

	if err := os.Rename(tmp, final); err != nil {
		return err
	}
	if err := syncDir(s.dir); err != nil {
		return err
	}

	// Update LATEST pointer atomically.
	latestTmp := filepath.Join(s.dir, "LATEST.tmp")
	if err := os.WriteFile(latestTmp, []byte(strconv.FormatInt(int64(meta.ID), 10)), 0o644); err != nil {
		return err
	}
	if err := os.Rename(latestTmp, filepath.Join(s.dir, "LATEST")); err != nil {
		return err
	}
	return syncDir(s.dir)
}

// loadLatest reads the checkpoint pointed at by LATEST, if any.
func (s *checkpointStore) loadLatest() (checkpointMeta, sourceState, []subtaskSnapshot, error) {
	var meta checkpointMeta
	var src sourceState

	data, err := os.ReadFile(filepath.Join(s.dir, "LATEST"))
	if err != nil {
		return meta, src, nil, err
	}
	id, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return meta, src, nil, err
	}
	dir := s.chkDir(checkpointID(id))

	readJSON := func(name string, v any) error {
		f, err := os.Open(filepath.Join(dir, name))
		if err != nil {
			return err
		}
		defer f.Close()
		return json.NewDecoder(f).Decode(v)
	}

	if err := readJSON("_metadata.json", &meta); err != nil {
		return meta, src, nil, err
	}
	if err := readJSON("source-0.state", &src); err != nil {
		return meta, src, nil, err
	}

	subs := make([]subtaskSnapshot, meta.Parallelism)
	for i := range subs {
		if err := readJSON(fmt.Sprintf("subtask-%d.state", i), &subs[i]); err != nil {
			return meta, src, nil, err
		}
	}
	return meta, src, subs, nil
}

// list returns completed checkpoint metadata, newest last.
func (s *checkpointStore) list() ([]checkpointMeta, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	var metas []checkpointMeta
	for _, e := range entries {
		if !e.IsDir() || len(e.Name()) < 4 || e.Name()[:4] != "chk-" {
			continue
		}
		f, err := os.Open(filepath.Join(s.dir, e.Name(), "_metadata.json"))
		if err != nil {
			continue
		}
		var m checkpointMeta
		if json.NewDecoder(f).Decode(&m) == nil {
			metas = append(metas, m)
		}
		f.Close()
	}
	sort.Slice(metas, func(i, j int) bool { return metas[i].ID < metas[j].ID })
	return metas, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
