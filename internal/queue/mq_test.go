package queue

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestMapQueueAddNewTask(t *testing.T) {
	mq := NewMapQueue()
	mt := &MapTask{MapID: 1, Filename: "file1.txt"}

	if err := mq.AddNewTask(mt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(mq.idle) != 1 {
		t.Fatalf("expected 1 idle task, got %d", len(mq.idle))
	}
	if mq.amount != 1 {
		t.Fatalf("expected amount=1, got %d", mq.amount)
	}

	// duplicate by MapID
	if err := mq.AddNewTask(&MapTask{MapID: 1, Filename: "x"}); err == nil {
		t.Fatal("expected error adding duplicate MapID task")
	}
}

func TestMapQueueFetchIdleTask(t *testing.T) {
	mq := NewMapQueue()
	_ = mq.AddNewTask(&MapTask{MapID: 2})

	task := mq.FetchIdleTask()
	if task == nil {
		t.Fatal("expected a task, got nil")
	}
	if task.MapID != 2 {
		t.Fatalf("expected MapID=2, got %d", task.MapID)
	}
	if task.AttemptID == uuid.Nil {
		t.Fatal("expected AttemptID to be set")
	}
	if time.Since(task.AttemptTime) > time.Second {
		t.Fatal("attempt time seems too old; may not have been set correctly")
	}
	if len(mq.idle) != 0 || len(mq.pending) != 1 {
		t.Fatalf("expected idle=0 & pending=1, got idle=%d pending=%d", len(mq.idle), len(mq.pending))
	}
}

func TestMapQueueCompleteTask(t *testing.T) {
	mq := NewMapQueue()
	_ = mq.AddNewTask(&MapTask{MapID: 3})
	taskFetched := mq.FetchIdleTask()
	if taskFetched == nil {
		t.Fatal("expected task")
	}

	mq.CompleteTask(taskFetched.AttemptID)

	if len(mq.pending) != 0 {
		t.Fatalf("expected pending=0, got %d", len(mq.pending))
	}
	if len(mq.completed) != 1 {
		t.Fatalf("expected completed=1, got %d", len(mq.completed))
	}
	if mq.completed[0].MapID != 3 {
		t.Fatalf("expected completed MapID=3, got %d", mq.completed[0].MapID)
	}
}

func TestMapQueueCompleteTaskWithWrongAttemptID(t *testing.T) {
	mq := NewMapQueue()
	_ = mq.AddNewTask(&MapTask{MapID: 30})
	taskFetched := mq.FetchIdleTask()
	if taskFetched == nil {
		t.Fatal("expected task")
	}

	// Use random attempt ID that does not match
	mq.CompleteTask(uuid.New())
	if len(mq.pending) != 1 || len(mq.completed) != 0 {
		t.Fatalf("expected pending=1 completed=0 after wrong completion, got %d %d", len(mq.pending), len(mq.completed))
	}

	// Now complete correctly
	mq.CompleteTask(taskFetched.AttemptID)
	if len(mq.pending) != 0 || len(mq.completed) != 1 {
		t.Fatalf("expected pending=0 completed=1 after correct completion, got %d %d", len(mq.pending), len(mq.completed))
	}
}

func TestMapQueueDone(t *testing.T) {
	mq := NewMapQueue()
	_ = mq.AddNewTask(&MapTask{MapID: 4})
	_ = mq.AddNewTask(&MapTask{MapID: 5})

	if mq.Done() {
		t.Fatal("expected Done() false initially")
	}
	t1 := mq.FetchIdleTask()
	t2 := mq.FetchIdleTask()
	if t1 == nil || t2 == nil {
		t.Fatal("expected two tasks fetched")
	}
	mq.CompleteTask(t1.AttemptID)
	if mq.Done() {
		t.Fatal("expected not done with one task completed")
	}
	mq.CompleteTask(t2.AttemptID)
	if !mq.Done() {
		t.Fatal("expected done after all tasks completed")
	}
}

func TestMapQueueRequeueAfterTimeout(t *testing.T) {
	oldTimeout := taskTimeout
	taskTimeout = 100 * time.Millisecond
	defer func() { taskTimeout = oldTimeout }()

	mq := NewMapQueue()
	_ = mq.AddNewTask(&MapTask{MapID: 6})
	first := mq.FetchIdleTask()
	if first == nil {
		t.Fatal("expected first fetch")
	}
	firstAttempt := first.AttemptID

	// Sleep long enough for: timeout to elapse and ticker (1s) to fire once.
	time.Sleep(1200 * time.Millisecond)

	// After requeue it should be back to idle and not pending
	if len(mq.pending) != 0 {
		t.Fatalf("expected pending=0 after requeue, got %d", len(mq.pending))
	}
	if len(mq.idle) != 1 {
		t.Fatalf("expected idle=1 after requeue, got %d", len(mq.idle))
	}
	if mq.idle[0].MapID != 6 {
		t.Fatalf("expected requeued MapID=6, got %d", mq.idle[0].MapID)
	}

	refetched := mq.FetchIdleTask()
	if refetched.AttemptID == firstAttempt {
		t.Fatal("expected new AttemptID after re-fetching requeued task")
	}
}
