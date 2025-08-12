package coordinator

import (
	"os"
	"slices"
	"strconv"

	"github.com/svaan1/map-reduce-go/internal/queue"
)

type GetTaskReply struct {
	MapTask     *queue.MapTask
	ReduceTask  *queue.ReduceTask
	ProcessDone bool
}

type CompleteMapTaskArgs struct {
	TaskID       int
	CreatedFiles map[int]string
}

type CompleteReduceTask struct {
	TaskID int
}

func (c *Coordinator) GetTask(_ struct{}, reply *GetTaskReply) error {
	if !c.mq.Done() {
		reply.MapTask = c.mq.FetchIdleTask()
	} else if !c.rq.Done() {
		reply.ReduceTask = c.rq.FetchIdleTask()
	}

	reply.ProcessDone = c.Done()

	return nil
}

func (c *Coordinator) CompleteMapTask(args CompleteMapTaskArgs, _ *struct{}) error {
	if c.mq.Done() {
		return nil
	}

	c.mq.CompleteTask(args.TaskID)

	c.mu.Lock()
	defer c.mu.Unlock()

	// add intermediate files to the list without duplicates
	for reduceIdx, filename := range args.CreatedFiles {
		list := c.intermediateFiles[reduceIdx]
		if !slices.Contains(list, filename) {
			list = append(list, filename)
			c.intermediateFiles[reduceIdx] = list
		}
	}

	return nil
}

func (c *Coordinator) CompleteReduceTask(args CompleteReduceTask, _ *struct{}) error {
	if c.rq.Done() {
		return nil
	}

	c.rq.CompleteTask(args.TaskID)
	return nil
}

func (c *Coordinator) Done() bool {
	return c.mq.Done() && c.rq.Done()
}

func CoordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
