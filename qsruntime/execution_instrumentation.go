package qsruntime

import (
	"context"
	"sync"
	"time"
)

// ExecutionInstrumentation records structured, request-scoped execution facts.
// It is intentionally independent of logging so SQLRunner, EXPLAIN ANALYZE,
// management APIs, and optimizer feedback can consume the same observations.
type ExecutionInstrumentation struct {
	mu       sync.Mutex
	timings  []ExecutionTiming
	counters []ExecutionCounter
	events   []ExecutionEvent
}

// ExecutionTiming records the elapsed time for one execution phase.
type ExecutionTiming struct {
	Section  string
	Name     string
	Duration time.Duration
	Detail   string
}

// ExecutionCounter records a numeric execution observation.
type ExecutionCounter struct {
	Section string
	Name    string
	Value   uint64
	Detail  string
}

// ExecutionEvent records a textual execution observation.
type ExecutionEvent struct {
	Section string
	Name    string
	Value   string
	Detail  string
}

// NewExecutionInstrumentation creates an empty request-scoped recorder.
func NewExecutionInstrumentation() *ExecutionInstrumentation {
	return &ExecutionInstrumentation{}
}

// ExecutionInstrumentationFromContext returns the request-scoped recorder, when installed.
func ExecutionInstrumentationFromContext(ctx context.Context) *ExecutionInstrumentation {
	scratchpad := QueryScratchpadFromContext(ctx)
	if scratchpad == nil {
		return nil
	}
	return scratchpad.Instrumentation
}

// ExecutionInstrumentationSnapshotFromContext returns a stable copy of the
// request-scoped instrumentation, when installed.
func ExecutionInstrumentationSnapshotFromContext(ctx context.Context) ExecutionInstrumentationSnapshot {
	recorder := ExecutionInstrumentationFromContext(ctx)
	if recorder == nil {
		return ExecutionInstrumentationSnapshot{}
	}
	return recorder.Snapshot()
}

// ObserveDuration records an elapsed execution phase.
func (i *ExecutionInstrumentation) ObserveDuration(section, name string, duration time.Duration, detail string) {
	if i == nil || section == "" || name == "" {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.timings = append(i.timings, ExecutionTiming{
		Section:  section,
		Name:     name,
		Duration: duration,
		Detail:   detail,
	})
}

// ObserveCount records a numeric execution observation.
func (i *ExecutionInstrumentation) ObserveCount(section, name string, value uint64, detail string) {
	if i == nil || section == "" || name == "" {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.counters = append(i.counters, ExecutionCounter{
		Section: section,
		Name:    name,
		Value:   value,
		Detail:  detail,
	})
}

// ObserveEvent records a textual execution observation.
func (i *ExecutionInstrumentation) ObserveEvent(section, name string, value string, detail string) {
	if i == nil || section == "" || name == "" {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.events = append(i.events, ExecutionEvent{
		Section: section,
		Name:    name,
		Value:   value,
		Detail:  detail,
	})
}

// Snapshot returns a stable copy of all recorded observations.
func (i *ExecutionInstrumentation) Snapshot() ExecutionInstrumentationSnapshot {
	if i == nil {
		return ExecutionInstrumentationSnapshot{}
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	return ExecutionInstrumentationSnapshot{
		Timings:  append([]ExecutionTiming(nil), i.timings...),
		Counters: append([]ExecutionCounter(nil), i.counters...),
		Events:   append([]ExecutionEvent(nil), i.events...),
	}
}

// ExecutionInstrumentationSnapshot is an immutable copy of recorded execution observations.
type ExecutionInstrumentationSnapshot struct {
	Timings  []ExecutionTiming
	Counters []ExecutionCounter
	Events   []ExecutionEvent
}

// Empty reports whether the snapshot contains no observations.
func (s ExecutionInstrumentationSnapshot) Empty() bool {
	return len(s.Timings) == 0 && len(s.Counters) == 0 && len(s.Events) == 0
}
