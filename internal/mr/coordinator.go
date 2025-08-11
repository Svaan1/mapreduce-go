package mr

import (
	"log"
)

type Coordinator struct {
	mq *MapQueue
	rq *ReduceQueue
}

type GetTaskReply struct {
	MapTask    *MapTask
	ReduceTask *ReduceTask
}

type CompleteTaskArgs struct {
	TaskID int
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mq: &MapQueue{},
		rq: &ReduceQueue{},
	}

	c.createMapTasks(files, nReduce)
	c.createReduceTasks(nReduce)
	c.server()
	return &c
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

func (c *Coordinator) createMapTasks(files []string, nReduce int) {
	for i, file := range files {
		task := &MapTask{
			ID:       i,
			NReduce:  nReduce,
			Filename: file,
		}

		if err := c.mq.AddNewTask(task); err != nil {
			log.Fatal(err)
		}
	}
}

func (c *Coordinator) createReduceTasks(nReduce int) {
	for i := range nReduce {
		task := &ReduceTask{
			ID: i,
		}

		if err := c.rq.AddNewTask(task); err != nil {
			log.Fatal(err)
		}
	}
}
