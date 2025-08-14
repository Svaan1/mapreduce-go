package queue

import (
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
)

type ReduceTask struct {
	AttemptID   uuid.UUID
	AttemptTime time.Time
	ReduceID    int
	Files       []string
}

type ReduceQueue struct {
	ids       []int
	idle      []*ReduceTask
	pending   []*ReduceTask
	completed []*ReduceTask

	amount  int
	started bool
	mu      sync.Mutex
}

func NewReduceQueue() *ReduceQueue {
	rq := ReduceQueue{
		ids:       []int{},
		idle:      []*ReduceTask{},
		pending:   []*ReduceTask{},
		completed: []*ReduceTask{},
		amount:    0,
		started:   false,
	}

	go rq.trackCompletions()

	return &rq
}

func (rq *ReduceQueue) Start() {
	rq.mu.Lock()
	defer rq.mu.Unlock()

	rq.started = true
}

func (rq *ReduceQueue) AddNewTask(mt *ReduceTask) error {
	rq.mu.Lock()
	defer rq.mu.Unlock()

	if slices.Contains(rq.ids, mt.ReduceID) {
		return fmt.Errorf("task with ReduceID %d already exists", mt.ReduceID)
	}

	rq.ids = append(rq.ids, mt.ReduceID)
	rq.idle = append(rq.idle, mt)
	rq.amount++

	return nil
}

func (rq *ReduceQueue) FetchIdleTask() *ReduceTask {
	rq.mu.Lock()
	defer rq.mu.Unlock()

	if len(rq.idle) == 0 {
		return nil
	}

	task := rq.idle[0]
	task.AttemptID = uuid.New()
	task.AttemptTime = time.Now()

	rq.idle = rq.idle[1:]
	rq.pending = append(rq.pending, task)

	return task
}

func (rq *ReduceQueue) CompleteTask(attemptID uuid.UUID) (ReduceTask, error) {
	rq.mu.Lock()
	defer rq.mu.Unlock()

	for i, task := range rq.pending {
		if task.AttemptID == attemptID {
			rq.completed = append(rq.completed, task)
			rq.pending = append(rq.pending[:i], rq.pending[i+1:]...)
			return *task, nil
		}
	}

	return ReduceTask{}, fmt.Errorf("stale attempt")
}

func (rq *ReduceQueue) Done() bool {
	rq.mu.Lock()
	defer rq.mu.Unlock()

	return rq.started &&
		len(rq.idle) == 0 &&
		len(rq.pending) == 0 &&
		len(rq.completed) == rq.amount
}

func (rq *ReduceQueue) trackCompletions() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C
		rq.mu.Lock()

		var fresh []*ReduceTask
		var stale []*ReduceTask

		for _, task := range rq.pending {
			if time.Since(task.AttemptTime) >= taskTimeout {
				stale = append(stale, task)
			} else {
				fresh = append(fresh, task)
			}
		}

		rq.pending = fresh
		rq.idle = append(rq.idle, stale...)

		rq.mu.Unlock()
	}
}
