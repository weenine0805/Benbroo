package health

import (
	"sync"
	"time"
)

// passiveFailureEvent records a single failure report from a consumer.
type passiveFailureEvent struct {
	timestamp time.Time
}

// passiveTracker tracks consumer-reported failures per instance
// using a sliding time window counter.
type passiveTracker struct {
	mu     sync.Mutex
	events map[uint64][]passiveFailureEvent // instanceID -> failure timestamps
}

func newPassiveTracker() *passiveTracker {
	return &passiveTracker{
		events: make(map[uint64][]passiveFailureEvent),
	}
}

// ReportFailure records a failure event for the given instance.
func (t *passiveTracker) ReportFailure(instanceID uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events[instanceID] = append(t.events[instanceID], passiveFailureEvent{
		timestamp: time.Now(),
	})
}

// ReportSuccess clears all failure events for the given instance
// (a successful call resets the failure counter).
func (t *passiveTracker) ReportSuccess(instanceID uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.events, instanceID)
}

// FailureCount returns the number of failures within the sliding window.
func (t *passiveTracker) FailureCount(instanceID uint64, window time.Duration) int {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-window)

	events := t.events[instanceID]
	// Prune old events.
	valid := make([]passiveFailureEvent, 0, len(events))
	count := 0
	for _, e := range events {
		if e.timestamp.After(cutoff) {
			valid = append(valid, e)
			count++
		}
	}
	t.events[instanceID] = valid
	return count
}

// Cleanup removes all events for the given instance.
func (t *passiveTracker) Cleanup(instanceID uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.events, instanceID)
}
