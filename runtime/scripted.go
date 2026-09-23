package runtime

import "context"

// ScriptedSource emits a fixed list of events in order.
type ScriptedSource struct {
	Events []Event
}

// Run implements Source.
func (s ScriptedSource) Run(_ context.Context, emit func(Event) error) error {
	for _, event := range s.Events {
		if err := emit(event); err != nil {
			return err
		}
	}
	return nil
}
