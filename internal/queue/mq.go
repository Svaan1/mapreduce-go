package queue

import (
	"fmt"
	"slices"
	"sync"
	"time"
)

var taskTimeout = 10 * time.Second

type MapTask struct {
	ID       int
	NReduce  int
	Filename string
}

type MapQueue struct {
	ids       []int
	idle      []*MapTask
	pending   []*MapTask
	completed []*MapTask

	amount int
	mu     sync.Mutex
}

func (mq *MapQueue) AddNewTask(mt *MapTask) error {
	mq.mu.Lock()
	defer mq.mu.Unlock()

	if slices.Contains(mq.ids, mt.ID) {
		return fmt.Errorf("task with ID %d already exists", mt.ID)
	}

	mq.ids = append(mq.ids, mt.ID)
	mq.idle = append(mq.idle, mt)
	mq.amount++

	return nil
}

func (mq *MapQueue) FetchIdleTask() *MapTask {
	mq.mu.Lock()
	defer mq.mu.Unlock()

	if len(mq.idle) == 0 {
		return nil
	}

	task := mq.idle[0]
	mq.idle = mq.idle[1:]
	mq.pending = append(mq.pending, task)

	go mq.trackCompletion(task)

	return task
}

func (mq *MapQueue) CompleteTask(ID int) {
	mq.mu.Lock()
	defer mq.mu.Unlock()

	for i, task := range mq.pending {
		if task.ID == ID {
			mq.completed = append(mq.completed, task)
			mq.pending = append(mq.pending[:i], mq.pending[i+1:]...)
			return
		}
	}
}

func (mq *MapQueue) Done() bool {
	mq.mu.Lock()
	defer mq.mu.Unlock()

	return len(mq.idle) == 0 &&
		len(mq.pending) == 0 &&
		len(mq.completed) == mq.amount
}

func (mq *MapQueue) trackCompletion(mt *MapTask) {
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
