package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

// MarchiqSource consumes a marchiq topic. Reads use explicit offsets (the
// position lives here and in checkpoints); the consumer-group commit is
// published only when a checkpoint completes, and it carries the group
// generation the broker fences commits with.
//
// Group fetch cannot do this: it always resumes at committed+1, so polling
// before the next checkpoint would re-read the same records.
type MarchiqSource struct {
	Broker   string // e.g. "http://localhost:9092"
	Topic    string
	Group    string
	Member   string
	MaxBatch int

	client     *http.Client
	partitions []int
	generation int

	mu sync.Mutex
	// offsets is the next offset to fetch per assigned partition. Restored
	// from a checkpoint on recovery; the broker's committed offset is not
	// the read cursor.
	offsets map[int]int64
	// pendingOffsets accumulates records fetched since the last completed
	// checkpoint. Committed only on checkpoint completion.
	pendingOffsets map[int]int64
}

// NewMarchiqSource joins the consumer group and returns a ready source.
func NewMarchiqSource(broker, topic, group, member string) (*MarchiqSource, error) {
	s := &MarchiqSource{
		Broker:         broker,
		Topic:          topic,
		Group:          group,
		Member:         member,
		MaxBatch:       100,
		client:         &http.Client{Timeout: 10 * time.Second},
		offsets:        make(map[int]int64),
		pendingOffsets: make(map[int]int64),
	}
	if err := s.rejoin(context.Background()); err != nil {
		return nil, err
	}
	return s, nil
}

type marchiqAssignment struct {
	Generation int   `json:"generation"`
	Partitions []int `json:"partitions"`
}

// rejoin registers the member and refreshes the generation fence. It does
// not touch fetch offsets: a rejoin after eviction must resume where the
// checkpoint (or the local cursor) says, not at the broker's commit.
func (s *MarchiqSource) rejoin(ctx context.Context) error {
	joinURL := fmt.Sprintf("%s/groups/%s/join?topic=%s&member=%s",
		s.Broker, url.PathEscape(s.Group), url.QueryEscape(s.Topic), url.QueryEscape(s.Member))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, joinURL, nil)
	if err != nil {
		return err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("join group: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("join group: %s: %s", resp.Status, body)
	}
	var asg marchiqAssignment
	if err := json.Unmarshal(body, &asg); err != nil {
		return fmt.Errorf("decode join response: %w", err)
	}
	if len(asg.Partitions) == 0 {
		asg.Partitions = []int{0}
	}
	s.mu.Lock()
	s.generation = asg.Generation
	s.partitions = asg.Partitions
	for _, p := range asg.Partitions {
		if _, ok := s.offsets[p]; !ok {
			s.offsets[p] = 0
		}
	}
	s.mu.Unlock()
	return nil
}

// heartbeat keeps the group session alive. The broker evicts a member that
// misses the session timeout, which bumps the generation and fences our
// commits. A 404/409 means we were evicted or fenced: rejoin.
func (s *MarchiqSource) heartbeat(ctx context.Context) {
	s.mu.Lock()
	gen := s.generation
	s.mu.Unlock()
	hbURL := fmt.Sprintf("%s/groups/%s/heartbeat?topic=%s&member=%s&generation=%d",
		s.Broker, url.PathEscape(s.Group), url.QueryEscape(s.Topic),
		url.QueryEscape(s.Member), gen)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, hbURL, nil)
	if err != nil {
		return
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusConflict || resp.StatusCode == http.StatusNotFound {
		_ = s.rejoin(ctx)
		return
	}
	var hr struct {
		Generation int `json:"generation"`
	}
	if json.NewDecoder(resp.Body).Decode(&hr) == nil && hr.Generation > 0 {
		s.mu.Lock()
		s.generation = hr.Generation
		s.mu.Unlock()
	}
}

type marchiqRecord struct {
	Offset      int64  `json:"offset"`
	TimestampNS int64  `json:"timestamp_ns"`
	Key         []byte `json:"key"`
	Value       []byte `json:"value"`
}

type marchiqExplicitFetch struct {
	Records    []marchiqRecord `json:"records"`
	NextOffset int64           `json:"next_offset"`
}

// Run implements Source. It polls each assigned partition from the local
// offset and emits new records. An empty poll waits briefly and retries.
func (s *MarchiqSource) Run(ctx context.Context, emit func(Event) error) error {
	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	go func() {
		// Well inside marchiq's default 10s session timeout.
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
				s.heartbeat(hbCtx)
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		s.mu.Lock()
		parts := append([]int(nil), s.partitions...)
		s.mu.Unlock()

		anyRecords := false
		for _, p := range parts {
			s.mu.Lock()
			offset := s.offsets[p]
			s.mu.Unlock()

			recs, err := s.fetchPartition(ctx, p, offset)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				time.Sleep(500 * time.Millisecond)
				continue
			}
			for _, rec := range recs {
				anyRecords = true
				event := Event{
					Key:       string(rec.Key),
					Value:     string(rec.Value),
					Timestamp: time.Unix(0, rec.TimestampNS).UTC(),
				}
				if err := emit(event); err != nil {
					return err
				}
				s.mu.Lock()
				s.offsets[p] = rec.Offset + 1
				s.pendingOffsets[p] = rec.Offset + 1
				s.mu.Unlock()
			}
		}

		if !anyRecords {
			if err := injectIdleBarrier(ctx); err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
}

func (s *MarchiqSource) fetchPartition(ctx context.Context, partition int, offset int64) ([]marchiqRecord, error) {
	fetchURL := fmt.Sprintf("%s/fetch?topic=%s&partition=%d&offset=%d&max_records=%d",
		s.Broker, url.QueryEscape(s.Topic), partition, offset, s.MaxBatch)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fetchURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch partition %d offset %d: %s: %s", partition, offset, resp.Status, body)
	}
	var fr marchiqExplicitFetch
	if err := json.Unmarshal(body, &fr); err != nil {
		return nil, fmt.Errorf("decode fetch response: %w", err)
	}
	return fr.Records, nil
}

// Offset returns the minimum next-fetch offset across partitions.
// Used by the checkpoint barrier to snapshot the source position.
func (s *MarchiqSource) Offset() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var min int64 = -1
	for _, off := range s.offsets {
		if min < 0 || off < min {
			min = off
		}
	}
	if min < 0 {
		return 0
	}
	return min
}

// Seek rewinds the source to a checkpointed offset.
func (s *MarchiqSource) Seek(offset int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for p := range s.offsets {
		s.offsets[p] = offset
	}
	s.pendingOffsets = make(map[int]int64)
}

// CommitOffsets commits pending offsets to marchiq. Called by the coordinator
// only after a checkpoint completes. The commit carries the group generation;
// a fenced commit rejoins once and retries.
func (s *MarchiqSource) CommitOffsets(ctx context.Context) error {
	s.mu.Lock()
	pending := make(map[int]int64, len(s.pendingOffsets))
	for k, v := range s.pendingOffsets {
		pending[k] = v
	}
	gen := s.generation
	s.mu.Unlock()

	if err := s.commit(ctx, pending, gen); err != nil {
		if err := s.rejoin(ctx); err != nil {
			return err
		}
		s.mu.Lock()
		gen = s.generation
		s.mu.Unlock()
		if err := s.commit(ctx, pending, gen); err != nil {
			return err
		}
	}
	s.mu.Lock()
	for p, off := range pending {
		if s.pendingOffsets[p] == off {
			delete(s.pendingOffsets, p)
		}
	}
	s.mu.Unlock()
	return nil
}

func (s *MarchiqSource) commit(ctx context.Context, pending map[int]int64, generation int) error {
	for partition, offset := range pending {
		body, _ := json.Marshal(map[string]any{
			"group":      s.Group,
			"topic":      s.Topic,
			"partition":  partition,
			"offset":     offset - 1, // commit = last processed, next fetch = offset
			"generation": generation,
		})
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			s.Broker+"/commit", bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := s.client.Do(req)
		if err != nil {
			return fmt.Errorf("commit partition %d offset %d: %w", partition, offset, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("commit partition %d offset %d: %s", partition, offset, resp.Status)
		}
	}
	return nil
}

// PendingOffsets returns a copy of offsets not yet committed.
func (s *MarchiqSource) PendingOffsets() map[int]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[int]int64, len(s.pendingOffsets))
	for k, v := range s.pendingOffsets {
		out[k] = v
	}
	return out
}

// OffsetMap returns a copy of current fetch positions.
func (s *MarchiqSource) OffsetMap() map[int]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[int]int64, len(s.offsets))
	for k, v := range s.offsets {
		out[k] = v
	}
	return out
}

// RestoreOffsets sets fetch positions from a checkpoint.
func (s *MarchiqSource) RestoreOffsets(offsets map[int]int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.offsets = make(map[int]int64, len(offsets))
	for k, v := range offsets {
		s.offsets[k] = v
	}
	s.pendingOffsets = make(map[int]int64)
}

// offsetString serializes the offset map for checkpoint storage.
func (s *MarchiqSource) offsetString() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, _ := json.Marshal(s.offsets)
	return string(data)
}

// restoreOffsetString deserializes offsets from checkpoint storage.
func (s *MarchiqSource) restoreOffsetString(data string) error {
	var offsets map[int]int64
	if err := json.Unmarshal([]byte(data), &offsets); err != nil {
		return err
	}
	s.RestoreOffsets(offsets)
	return nil
}

// parseOffset is a helper for tests.
func parseOffset(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}
