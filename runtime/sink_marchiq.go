package runtime

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// MarchiqSink produces windowed results back to a marchiq topic.
type MarchiqSink struct {
	Broker string
	Topic  string

	client *http.Client
}

// NewMarchiqSink creates a sink that produces to the given topic.
func NewMarchiqSink(broker, topic string) *MarchiqSink {
	return &MarchiqSink{
		Broker: broker,
		Topic:  topic,
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

// Write implements Sink.
func (s *MarchiqSink) Write(ctx context.Context, event Event) error {
	produceURL := fmt.Sprintf("%s/produce?topic=%s&partition=-1&key=%s",
		s.Broker, url.QueryEscape(s.Topic), url.QueryEscape(event.Key))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, produceURL,
		bytes.NewReader([]byte(event.Value)))
	if err != nil {
		return err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("produce to %s: %w", s.Topic, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("produce to %s: %s: %s", s.Topic, resp.Status, body)
	}
	return nil
}
