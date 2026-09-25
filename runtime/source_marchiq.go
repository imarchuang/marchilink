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
	"time"
)

// MarchiqSource consumes records from a marchiq topic via consumer-group
// fetch. Offsets are checkpointed with the rest of the graph state and
// committed to marchiq only when a checkpoint completes.
type MarchiqSource struct {
	Broker   string // e.g. "http://localhost:9092"
	Topic    string
	Group    string
	Member   string
	MaxBatch int

	client *http.Client

	// offsets tracks the next offset to fetch per partition. Restored from
	// checkpoint on recovery.
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

	// Join the group to get partition assignment.
	joinURL := fmt.Sprintf("%s/groups/%s/join?topic=%s&member=%s&members=1",
		s.Broker, url.PathEscape(s.Group), url.QueryEscape(s.Topic), url.QueryEscape(s.Member))
	resp, err := s.client.Post(joinURL, "application/json", nil)
	if err != nil {
		return nil, fmt.Errorf("join group: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("join group: %s: %s", resp.Status, body)
	}

	return s, nil
}

// marchiqFetchResponse mirrors the marchiq group fetch JSON.
type marchiqFetchResponse struct {
	Partitions []struct {
		Partition int `json:"partition"`
		Records   []struct {
			Offset      int64  `json:"offset"`
			TimestampNS int64  `json:"timestamp_ns"`
			Key         []byte `json:"key"`
			Value       []byte `json:"value"`
		} `json:"records"`
		NextOffset    int64 `json:"next_offset"`
		HighWatermark int64 `json:"high_watermark"`
	} `json:"partitions"`
}

// Run implements Source. It polls marchiq for new records and emits them.
func (s *MarchiqSource) Run(ctx context.Context, emit func(Event) error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		fetchURL := fmt.Sprintf("%s/fetch?group=%s&topic=%s&member=%s&max_records=%d",
			s.Broker, url.QueryEscape(s.Group), url.QueryEscape(s.Topic),
			url.QueryEscape(s.Member), s.MaxBatch)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fetchURL, nil)
		if err != nil {
			return err
		}
		resp, err := s.client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Broker not ready yet; retry.
			time.Sleep(500 * time.Millisecond)
			continue
		}

		var fr marchiqFetchResponse
		if err := json.NewDecoder(resp.Body).Decode(&fr); err != nil {
			resp.Body.Close()
			return fmt.Errorf("decode fetch response: %w", err)
		}
		resp.Body.Close()

		anyRecords := false
		for _, p := range fr.Partitions {
			for _, rec := range p.Records {
				anyRecords = true
				event := Event{
					Key:       string(rec.Key),
					Value:     string(rec.Value),
					Timestamp: time.Unix(0, rec.TimestampNS).UTC(),
				}
				if err := emit(event); err != nil {
					return err
				}
				// Track the next offset to fetch.
				s.offsets[p.Partition] = rec.Offset + 1
				s.pendingOffsets[p.Partition] = rec.Offset + 1
			}
		}

		if !anyRecords {
			// No new records; poll again after a short delay.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
}

// Offset returns the current fetch position (next offset across partitions).
// Used by the checkpoint barrier to snapshot the source position.
func (s *MarchiqSource) Offset() int64 {
	// For single-partition topics this is exact. For multi-partition we
	// return the minimum across partitions (conservative: replay a few extra
	// records rather than skip any).
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
	for p := range s.offsets {
		s.offsets[p] = offset
	}
	// Clear pending: we replay from the checkpoint, so anything fetched but
	// not yet committed is discarded.
	s.pendingOffsets = make(map[int]int64)
}

// CommitOffsets commits pending offsets to marchiq. Called by the coordinator
// only after a checkpoint completes.
func (s *MarchiqSource) CommitOffsets(ctx context.Context) error {
	for partition, offset := range s.pendingOffsets {
		body, _ := json.Marshal(map[string]any{
			"group":     s.Group,
			"topic":     s.Topic,
			"partition": partition,
			"offset":    offset - 1, // commit = last processed, next fetch = offset
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
	// Clear committed offsets.
	s.pendingOffsets = make(map[int]int64)
	return nil
}

// PendingOffsets returns a copy of offsets not yet committed.
func (s *MarchiqSource) PendingOffsets() map[int]int64 {
	out := make(map[int]int64, len(s.pendingOffsets))
	for k, v := range s.pendingOffsets {
		out[k] = v
	}
	return out
}

// OffsetMap returns a copy of current fetch positions.
func (s *MarchiqSource) OffsetMap() map[int]int64 {
	out := make(map[int]int64, len(s.offsets))
	for k, v := range s.offsets {
		out[k] = v
	}
	return out
}

// RestoreOffsets sets fetch positions from a checkpoint.
func (s *MarchiqSource) RestoreOffsets(offsets map[int]int64) {
	s.offsets = make(map[int]int64, len(offsets))
	for k, v := range offsets {
		s.offsets[k] = v
	}
	s.pendingOffsets = make(map[int]int64)
}

// offsetString serializes the offset map for checkpoint storage.
func (s *MarchiqSource) offsetString() string {
	data, _ := json.Marshal(s.offsets)
	return string(data)
}

// restoreOffsetString deserializes offsets from checkpoint storage.
func (s *MarchiqSource) restoreOffsetString(data string) error {
	return json.Unmarshal([]byte(data), &s.offsets)
}

// parseOffset is a helper for tests.
func parseOffset(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}
