package worker

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestRunLockerTryAcquireAndRelease(t *testing.T) {
	locker := NewRunLocker()

	release, ok := locker.TryAcquire("run-1")
	if !ok {
		t.Fatal("first acquire failed")
	}
	if _, ok := locker.TryAcquire("run-1"); ok {
		t.Fatal("second acquire must fail while held")
	}

	// Different runs use different locks.
	releaseOther, ok := locker.TryAcquire("run-2")
	if !ok {
		t.Fatal("different run should acquire independently")
	}
	releaseOther()

	release()
	release() // idempotent

	releaseAgain, ok := locker.TryAcquire("run-1")
	if !ok {
		t.Fatal("acquire after release failed")
	}
	releaseAgain()
}

func TestRunLockerOnlyOneConcurrentWinner(t *testing.T) {
	locker := NewRunLocker()

	const contenders = 32
	var (
		successes  atomic.Int32
		attempted  sync.WaitGroup
		goroutines sync.WaitGroup
	)

	attempted.Add(contenders)
	goroutines.Add(contenders)
	allowRelease := make(chan struct{})

	for i := 0; i < contenders; i++ {
		go func() {
			defer goroutines.Done()

			release, ok := locker.TryAcquire("same-run")
			attempted.Done()
			if !ok {
				return
			}

			successes.Add(1)
			<-allowRelease
			release()
		}()
	}

	// Keep the winner holding the lock until every contender has tried.
	attempted.Wait()
	if got := successes.Load(); got != 1 {
		t.Fatalf("successful acquisitions = %d, want 1", got)
	}
	close(allowRelease)
	goroutines.Wait()
}
