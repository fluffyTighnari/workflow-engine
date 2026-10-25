package queue

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBoundedQueueFIFO(t *testing.T) {
	q := NewBoundedQueue(4)
	defer q.Close()

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		job := Job{ID: string(rune('a' + i)), RunID: "run-" + string(rune('a'+i)), Type: JobExecute}
		if err := q.Enqueue(ctx, job); err != nil {
			t.Fatalf("Enqueue #%d: %v", i, err)
		}
	}

	if got := q.Len(); got != 3 {
		t.Fatalf("Len = %d, want 3", got)
	}

	for i := 0; i < 3; i++ {
		job, err := q.Dequeue(ctx)
		if err != nil {
			t.Fatalf("Dequeue #%d: %v", i, err)
		}
		want := "run-" + string(rune('a'+i))
		if job.RunID != want {
			t.Errorf("Dequeue #%d RunID = %q, want %q", i, job.RunID, want)
		}
	}
}

func TestBoundedQueueEnqueueBlocksWhenFull(t *testing.T) {
	q := NewBoundedQueue(1)
	defer q.Close()

	ctx := context.Background()
	if err := q.Enqueue(ctx, Job{ID: "1", RunID: "run-1"}); err != nil {
		t.Fatal(err)
	}

	// 容量已满：带超时 ctx 的 Enqueue 应阻塞到超时返回。
	timeoutCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := q.Enqueue(timeoutCtx, Job{ID: "2", RunID: "run-2"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Enqueue on full queue = %v, want DeadlineExceeded", err)
	}
	if time.Since(start) < 40*time.Millisecond {
		t.Fatal("Enqueue returned too fast, it should have blocked")
	}

	// 释放一个位置后 Enqueue 立即成功。
	_, err = q.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(ctx, Job{ID: "2", RunID: "run-2"}); err != nil {
		t.Fatalf("Enqueue after dequeue: %v", err)
	}
}

func TestBoundedQueueDequeueBlocksWhenEmpty(t *testing.T) {
	q := NewBoundedQueue(2)
	defer q.Close()

	timeoutCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := q.Dequeue(timeoutCtx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Dequeue on empty queue = %v, want DeadlineExceeded", err)
	}
}

func TestBoundedQueueClose(t *testing.T) {
	q := NewBoundedQueue(2)

	// 先放一个任务，再关闭：Dequeue 应能取到已入队任务。
	ctx := context.Background()
	if err := q.Enqueue(ctx, Job{ID: "1", RunID: "run-1"}); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	job, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatalf("Dequeue after close with pending item: %v", err)
	}
	if job.RunID != "run-1" {
		t.Errorf("RunID = %q, want run-1", job.RunID)
	}

	// 队列清空后，Dequeue 返回 ErrClosed。
	_, err = q.Dequeue(ctx)
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("Dequeue on drained closed queue = %v, want ErrClosed", err)
	}

	// 关闭后 Enqueue 返回 ErrClosed。
	if err := q.Enqueue(ctx, Job{ID: "2", RunID: "run-2"}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Enqueue after close = %v, want ErrClosed", err)
	}

	// 重复 Close 是安全的。
	if err := q.Close(); err != nil {
		t.Fatalf("double Close: %v", err)
	}
}
