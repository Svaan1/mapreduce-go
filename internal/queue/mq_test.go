package queue

import (
	"testing"
	"time"
)

func TestAddNewTask(t *testing.T) {
	mq := &MapQueue{}
	task := &MapTask{ID: 1, Filename: "file1.txt"}

	if err := mq.AddNewTask(task); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(mq.idle) != 1 {
		t.Fatalf("expected 1 idle task, got %d", len(mq.idle))
	}
	if mq.amount != 1 {
		t.Fatalf("expected amount=1, got %d", mq.amount)
	}

	// add duplicate
	if err := mq.AddNewTask(task); err == nil {
		t.Fatal("expected error when adding duplicate task")
	}
}

func TestFetchIdleTask(t *testing.T) {
	mq := &MapQueue{}
	task := &MapTask{ID: 2}

	_ = mq.AddNewTask(task)
	got := mq.FetchIdleTask()

	if got == nil {
		t.Fatal("expected a task, got nil")
	}
	if got.ID != 2 {
		t.Fatalf("expected ID 2, got %d", got.ID)
	}
	if len(mq.idle) != 0 {
		t.Fatalf("expected idle=0, got %d", len(mq.idle))
	}
	if len(mq.pending) != 1 {
		t.Fatalf("expected pending=1, got %d", len(mq.pending))
	}
}

func TestCompleteTask(t *testing.T) {
	mq := &MapQueue{}
	task := &MapTask{ID: 3}

	_ = mq.AddNewTask(task)
	_ = mq.FetchIdleTask()
	mq.CompleteTask(3)

	if len(mq.pending) != 0 {
		t.Fatalf("expected pending=0, got %d", len(mq.pending))
	}
	if len(mq.completed) != 1 {
		t.Fatalf("expected completed=1, got %d", len(mq.completed))
	}
	if mq.completed[0].ID != 3 {
		t.Fatalf("expected completed ID=3, got %d", mq.completed[0].ID)
	}
}

func TestDone(t *testing.T) {
	mq := &MapQueue{}
	t1 := &MapTask{ID: 4}
	t2 := &MapTask{ID: 5}

	_ = mq.AddNewTask(t1)
	_ = mq.AddNewTask(t2)

	// not done yet
	if mq.Done() {
		t.Fatal("expected Done() to be false initially")
	}

	// process both
	_ = mq.FetchIdleTask()
	_ = mq.FetchIdleTask()
	mq.CompleteTask(4)
	mq.CompleteTask(5)

	if !mq.Done() {
		t.Fatal("expected Done() to be true after completion")
	}
}

func TestTrackRequeuesAfterTimeout(t *testing.T) {
	// shorten timeout for test
	oldTimeout := taskTimeout
	taskTimeout = 50 * time.Millisecond
	defer func() { taskTimeout = oldTimeout }()

	mq := &MapQueue{}
	task := &MapTask{ID: 6}

	_ = mq.AddNewTask(task)
	_ = mq.FetchIdleTask()

	time.Sleep(2 * taskTimeout)

	if len(mq.pending) != 0 {
		t.Fatalf("expected pending=0, got %d", len(mq.pending))
	}
	if len(mq.idle) != 1 {
		t.Fatalf("expected idle=1, got %d", len(mq.idle))
	}
	if mq.idle[0].ID != 6 {
		t.Fatalf("expected requeued ID=6, got %d", mq.idle[0].ID)
	}
}
