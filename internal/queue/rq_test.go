package queue

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestReduceQueueAddNewTask(t *testing.T) {
	rq := NewReduceQueue()
	rt := &ReduceTask{ReduceID: 1, Files: []string{"mr-0-1", "mr-1-1"}}
	if err := rq.AddNewTask(rt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rq.idle) != 1 {
		t.Fatalf("expected idle=1 got %d", len(rq.idle))
	}
	if rq.amount != 1 {
		t.Fatalf("expected amount=1 got %d", rq.amount)
	}
	if err := rq.AddNewTask(&ReduceTask{ReduceID: 1}); err == nil {
		t.Fatal("expected duplicate error")
	}
}

func TestReduceQueueStartAndDone(t *testing.T) {
	rq := NewReduceQueue()
	_ = rq.AddNewTask(&ReduceTask{ReduceID: 2})
	if rq.Done() {
		t.Fatal("expected Done false before start")
	}
	rq.Start()
	if rq.Done() {
		t.Fatal("expected Done false before processing")
	}
	task := rq.FetchIdleTask()
	if task == nil {
		t.Fatal("expected task")
	}
	rq.CompleteTask(task.AttemptID)
	if !rq.Done() {
		t.Fatal("expected Done true after completion")
	}
}

func TestReduceQueueFetchIdleTask(t *testing.T) {
	rq := NewReduceQueue()
	rq.Start()
	_ = rq.AddNewTask(&ReduceTask{ReduceID: 3})
	task := rq.FetchIdleTask()
	if task == nil {
		t.Fatal("expected task")
	}
	if task.ReduceID != 3 {
		t.Fatalf("expected ReduceID=3 got %d", task.ReduceID)
	}
	if task.AttemptID == uuid.Nil {
		t.Fatal("expected AttemptID set")
	}
	if time.Since(task.AttemptTime) > time.Second {
		t.Fatal("attempt time too old")
	}
	if len(rq.idle) != 0 || len(rq.pending) != 1 {
		t.Fatalf("expected idle=0 pending=1 got %d %d", len(rq.idle), len(rq.pending))
	}
}

func TestReduceQueueCompleteWrongAttempt(t *testing.T) {
	rq := NewReduceQueue()
	rq.Start()
	_ = rq.AddNewTask(&ReduceTask{ReduceID: 4})
	task := rq.FetchIdleTask()
	if task == nil {
		t.Fatal("expected task")
	}
	rq.CompleteTask(uuid.New()) // wrong attempt id
	if len(rq.pending) != 1 || len(rq.completed) != 0 {
		t.Fatalf("expected pending=1 completed=0 got %d %d", len(rq.pending), len(rq.completed))
	}
	rq.CompleteTask(task.AttemptID) // correct
	if len(rq.pending) != 0 || len(rq.completed) != 1 {
		t.Fatalf("expected pending=0 completed=1 got %d %d", len(rq.pending), len(rq.completed))
	}
}

func TestReduceQueueRequeueAfterTimeout(t *testing.T) {
	oldTimeout := taskTimeout
	taskTimeout = 100 * time.Millisecond
	defer func() { taskTimeout = oldTimeout }()

	rq := NewReduceQueue()
	rq.Start()
	_ = rq.AddNewTask(&ReduceTask{ReduceID: 5})
	first := rq.FetchIdleTask()
	if first == nil {
		t.Fatal("expected first fetch")
	}
	firstAttempt := first.AttemptID

	// wait longer than timeout + ticker interval (1s)
	time.Sleep(1200 * time.Millisecond)
	if len(rq.pending) != 0 {
		t.Fatalf("expected pending=0 got %d", len(rq.pending))
	}
	if len(rq.idle) != 1 {
		t.Fatalf("expected idle=1 got %d", len(rq.idle))
	}
	if rq.idle[0].ReduceID != 5 {
		t.Fatalf("expected ReduceID=5 got %d", rq.idle[0].ReduceID)
	}
	refetched := rq.FetchIdleTask()
	if refetched.AttemptID == firstAttempt {
		t.Fatal("expected new attempt id after requeue")
	}
}
