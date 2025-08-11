package coordinator

import (
	"os"
	"strconv"

	"github.com/svaan1/map-reduce-go/internal/queue"
)

type GetTaskReply struct {
	MapTask    *queue.MapTask
	ReduceTask *queue.ReduceTask
}

type CompleteTaskArgs struct {
	TaskID int
}

func (c *Coordinator) GetTask(_ struct{}, reply *GetTaskReply) error {
	if !c.mq.Done() {
		reply.MapTask = c.mq.FetchIdleTask()
	} else if !c.rq.Done() {
		reply.ReduceTask = c.rq.FetchIdleTask()
	}

	return nil
}

func (c *Coordinator) CompleteMapTask(args CompleteTaskArgs, _ *struct{}) error {
	c.mq.CompleteTask(args.TaskID)
	return nil
}

func (c *Coordinator) CompleteReduceTask(args CompleteTaskArgs, _ *struct{}) error {
	c.rq.CompleteTask(args.TaskID)
	return nil
}

func (c *Coordinator) Done() bool {
	return false
}

func CoordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
