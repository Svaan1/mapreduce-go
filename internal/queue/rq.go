package queue

import (
	"fmt"
	"slices"
	"sync"
	"time"
)

type ReduceTask struct {
	ID int
}

type ReduceQueue struct {
	ids       []int
	idle      []*ReduceTask
	pending   []*ReduceTask
	completed []*ReduceTask

	amount int
	mu     sync.Mutex
}

func (rq *ReduceQueue) AddNewTask(mt *ReduceTask) error {
	rq.mu.Lock()
	defer rq.mu.Unlock()

	if slices.Contains(rq.ids, mt.ID) {
		return fmt.Errorf("task with ID %d already exists", mt.ID)
	}

	rq.ids = append(rq.ids, mt.ID)
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
	rq.idle = rq.idle[1:]
	rq.pending = append(rq.pending, task)

	go rq.trackCompletion(task)

	return task
}

func (rq *ReduceQueue) CompleteTask(ID int) {
	rq.mu.Lock()
	defer rq.mu.Unlock()

	for i, task := range rq.pending {
		if task.ID == ID {
			rq.completed = append(rq.completed, task)
			rq.pending = append(rq.pending[:i], rq.pending[i+1:]...)
			break
		}
	}
}

func (rq *ReduceQueue) Done() bool {
	rq.mu.Lock()
	defer rq.mu.Unlock()

	return len(rq.idle) == 0 &&
		len(rq.pending) == 0 &&
		len(rq.completed) == rq.amount
}

func (mq *ReduceQueue) trackCompletion(mt *ReduceTask) {
	timer := time.NewTimer(taskTimeout)
	defer timer.Stop()
	<-timer.C

	mq.mu.Lock()
	defer mq.mu.Unlock()

	for i, task := range mq.pending {
		if task.ID == mt.ID {
			mq.pending = append(mq.pending[:i], mq.pending[i+1:]...)
			mq.idle = append(mq.idle, mt)
			return
		}
	}
}
