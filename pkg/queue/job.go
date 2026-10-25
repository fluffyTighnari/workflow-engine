package queue

import (
	"errors"
	"time"
)

// JobType 标识任务的类别，决定 worker 的处理分支。
type JobType string

const (
	// JobExecute 表示正常执行一个 run（创建 run 后入队）。
	JobExecute JobType = "execute"
	// JobRecover 表示崩溃恢复：修复 run 状态后继续执行
	// （进程启动时扫描未完成任务生成）。
	JobRecover JobType = "recover"
)

// Job 是消息队列中的最小任务单元。
type Job struct {
	// ID 用于日志与去重观测，通常复用 runID。
	ID string
	// RunID 指向要执行的 workflow run。
	RunID string
	// Type 决定 worker 的处理分支。
	Type JobType
	// CreatedAt 记录入队时间。
	CreatedAt time.Time
}

// ErrClosed 表示队列已关闭，不可再投递或接收。
var ErrClosed = errors.New("queue is closed")
