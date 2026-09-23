package runtime

import "time"

// Event is the unit of data that flows through a marchilink job.
type Event struct {
	Key       string
	Value     string
	Timestamp time.Time
}
