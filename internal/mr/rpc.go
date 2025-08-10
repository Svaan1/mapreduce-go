package mr

import (
	"os"
	"strconv"
)

//
// RPC definitions.
//
// remember to capitalize all names.
//

type GetTaskReply struct {
	MapTask    *MapTask
	ReduceTask *ReduceTask
}

func (c *Coordinator) GetTask(_ struct{}, reply *GetTaskReply) error {
	reply.MapTask = c.MQ.FetchIdleTask()
	reply.ReduceTask = nil

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
