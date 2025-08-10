package mr

import (
	"os"
	"strconv"
)

type GetTaskReply struct {
	MapTask    *MapTask
	ReduceTask *ReduceTask
}

type CompleteTaskArgs struct {
	TaskID int
}

func (c *Coordinator) GetTask(_ struct{}, reply *GetTaskReply) error {
	reply.MapTask = c.MQ.FetchIdleTask()
	reply.ReduceTask = nil
	return nil
}

func (c *Coordinator) CompleteTask(args CompleteTaskArgs, _ *struct{}) error {
	c.MQ.CompleteTask(args.TaskID)
	return nil
}

func (c *Coordinator) Done() bool {
	return false
}

func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
