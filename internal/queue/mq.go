package queue

import (
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
)

var taskTimeout = 10 * time.Second

type MapTask struct {
	AttemptID   uuid.UUID
	AttemptTime time.Time
	MapID       int
	NReduce     int
	Filename    string
}

type MapQueue struct {
	ids       []int
	idle      []*MapTask
	pending   []*MapTask
	completed []*MapTask

	amount int
	mu     sync.Mutex
}

func NewMapQueue() *MapQueue {
	mq := MapQueue{
		ids:       []int{},
		idle:      []*MapTask{},
		pending:   []*MapTask{},
		completed: []*MapTask{},
		amount:    0,
		mu:        sync.Mutex{},
	}

	go mq.trackCompletions()

	return &mq
}

func (mq *MapQueue) AddNewTask(mt *MapTask) error {
	mq.mu.Lock()
	defer mq.mu.Unlock()

	if slices.Contains(mq.ids, mt.MapID) {
		return fmt.Errorf("task with MapID %d already exists", mt.MapID)
	}

	mq.ids = append(mq.ids, mt.MapID)
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
	task.AttemptID = uuid.New()
	task.AttemptTime = time.Now()

	mq.idle = mq.idle[1:]
	mq.pending = append(mq.pending, task)

	return task
}

func (mq *MapQueue) CompleteTask(attemptID uuid.UUID) (MapTask, error) {
	mq.mu.Lock()
	defer mq.mu.Unlock()

	for i, task := range mq.pending {
		if task.AttemptID == attemptID {
			mq.completed = append(mq.completed, task)
			mq.pending = append(mq.pending[:i], mq.pending[i+1:]...)
			return *task, nil
		}
	}

	return MapTask{}, fmt.Errorf("stale attempt")
}

func (mq *MapQueue) Done() bool {
	mq.mu.Lock()
	defer mq.mu.Unlock()

	return len(mq.idle) == 0 &&
		len(mq.pending) == 0 &&
		len(mq.completed) == mq.amount
}

func (mq *MapQueue) trackCompletions() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C
		mq.mu.Lock()

		var fresh []*MapTask
		var stale []*MapTask

		for _, task := range mq.pending {
			if time.Since(task.AttemptTime) >= taskTimeout {
				stale = append(stale, task)
			} else {
				fresh = append(fresh, task)
			}
		}

		mq.pending = fresh
		mq.idle = append(mq.idle, stale...)

		mq.mu.Unlock()
	}
}
