package queue

import (
	"context"
	"sync"
)

// Queue 是进程内消息队列的契约。
// 目前由 BoundedQueue 实现；后续若引入持久化队列或外部 MQ，
// 只需提供新的实现，调用方（worker、server）无需改动。
type Queue interface {
	// Enqueue 投递一个 Job。队列满时阻塞等待空闲容量，
	// 直到 ctx 取消或队列关闭。
	Enqueue(ctx context.Context, job Job) error
	// Dequeue 取出一个 Job。队列空时阻塞等待，
	// 直到 ctx 取消或队列关闭。
	Dequeue(ctx context.Context) (Job, error)
	// Len 返回当前积压的任务数。
	Len() int
	// Close 关闭队列：唤醒所有阻塞的 Dequeue，并拒绝后续 Enqueue。
	Close() error
}

// BoundedQueue 是有界 FIFO 队列，底层为带容量上限的 channel。
//
// 语义：
//   - Enqueue 在队列满时阻塞（形成背压，不丢弃任务）；
//   - Dequeue 在队列空时阻塞；
//   - Close 后 Dequeue 先消费完已入队任务，再返回 ErrClosed；
//   - Close 后 Enqueue 返回 ErrClosed。
//
// Close 不关闭 jobs channel，而是关闭 done channel，因此 Enqueue、
// Dequeue、Close 可以从多个 goroutine 并发调用而不会 panic。
type BoundedQueue struct {
	jobs      chan Job
	done      chan struct{}
	closeOnce sync.Once
}

// NewBoundedQueue 创建容量为 capacity 的有界队列。
func NewBoundedQueue(capacity int) *BoundedQueue {
	if capacity <= 0 {
		capacity = 1
	}
	return &BoundedQueue{
		jobs: make(chan Job, capacity),
		done: make(chan struct{}),
	}
}

// Enqueue 投递一个 Job。队列满时阻塞直到有空闲容量。
func (q *BoundedQueue) Enqueue(ctx context.Context, job Job) error {
	select {
	case <-q.done:
		return ErrClosed
	default:
	}

	select {
	case q.jobs <- job:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-q.done:
		return ErrClosed
	}
}

// Dequeue 取出一个 Job。队列空时阻塞等待。
func (q *BoundedQueue) Dequeue(ctx context.Context) (Job, error) {
	// Prefer buffered jobs so Close drains instead of discarding.
	select {
	case job := <-q.jobs:
		return job, nil
	default:
	}

	select {
	case job := <-q.jobs:
		return job, nil
	case <-ctx.Done():
		return Job{}, ctx.Err()
	case <-q.done:
		// A job may have been buffered concurrently with Close.
		select {
		case job := <-q.jobs:
			return job, nil
		default:
			return Job{}, ErrClosed
		}
	}
}

// Len 返回当前积压的任务数。
func (q *BoundedQueue) Len() int {
	return len(q.jobs)
}

// Close 关闭队列并唤醒所有阻塞的 Dequeue。
func (q *BoundedQueue) Close() error {
	q.closeOnce.Do(func() {
		close(q.done)
	})
	return nil
}
