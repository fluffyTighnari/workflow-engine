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
//   - Close 后 Dequeue 返回 ErrClosed，Enqueue 返回 ErrClosed。
type BoundedQueue struct {
	jobs   chan Job
	mu     sync.Mutex
	closed bool
}

// NewBoundedQueue 创建容量为 capacity 的有界队列。
func NewBoundedQueue(capacity int) *BoundedQueue {
	if capacity <= 0 {
		capacity = 1
	}
	return &BoundedQueue{
		jobs: make(chan Job, capacity),
	}
}

// Enqueue 投递一个 Job。队列满时阻塞直到有空闲容量。
func (q *BoundedQueue) Enqueue(ctx context.Context, job Job) error {
	q.mu.Lock()
	closed := q.closed
	q.mu.Unlock()
	if closed {
		return ErrClosed
	}

	select {
	case q.jobs <- job:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Dequeue 取出一个 Job。队列空时阻塞等待。
func (q *BoundedQueue) Dequeue(ctx context.Context) (Job, error) {
	select {
	case job, ok := <-q.jobs:
		if !ok {
			return Job{}, ErrClosed
		}
		return job, nil
	case <-ctx.Done():
		return Job{}, ctx.Err()
	}
}

// Len 返回当前积压的任务数。
func (q *BoundedQueue) Len() int {
	return len(q.jobs)
}

// Close 关闭队列并唤醒所有阻塞的 Dequeue。
func (q *BoundedQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil
	}
	q.closed = true
	close(q.jobs)
	return nil
}
