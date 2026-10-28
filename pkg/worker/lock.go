package worker

import "sync"

// RunLocker guarantees that one run is processed by at most one worker
// at a time. The current implementation is process-local.
type RunLocker interface {
	// TryAcquire attempts to take the lock for runID. It returns a
	// release function and true on success. The release function is
	// idempotent.
	TryAcquire(runID string) (release func(), ok bool)
}

// MemoryRunLocker is an in-process RunLocker backed by a keyed mutex.
// It is safe for concurrent use.
type MemoryRunLocker struct {
	mu   sync.Mutex
	held map[string]struct{}
}

// NewRunLocker returns an empty process-local RunLocker.
func NewRunLocker() *MemoryRunLocker {
	return &MemoryRunLocker{held: make(map[string]struct{})}
}

// TryAcquire takes the run lock without blocking.
func (l *MemoryRunLocker) TryAcquire(runID string) (func(), bool) {
	l.mu.Lock()
	if _, exists := l.held[runID]; exists {
		l.mu.Unlock()
		return nil, false
	}
	l.held[runID] = struct{}{}
	l.mu.Unlock()

	var once sync.Once
	release := func() {
		once.Do(func() {
			l.mu.Lock()
			delete(l.held, runID)
			l.mu.Unlock()
		})
	}
	return release, true
}
