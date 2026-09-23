package runtime

import (
	"context"
	"fmt"
	"io"
)

// StdoutSink writes events to an output stream.
type StdoutSink struct {
	Writer io.Writer
}

// Write implements Sink.
func (s StdoutSink) Write(_ context.Context, event Event) error {
	if s.Writer == nil {
		return fmt.Errorf("stdout sink writer is nil")
	}

	_, err := fmt.Fprintf(s.Writer, "%s key=%s value=%s\n", event.Timestamp.Format("15:04:05.000"), event.Key, event.Value)
	return err
}
