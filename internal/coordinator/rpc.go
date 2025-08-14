package coordinator

import (
	"os"
	"strconv"

	"github.com/google/uuid"
	"github.com/svaan1/map-reduce-go/internal/common"
	"github.com/svaan1/map-reduce-go/internal/queue"
)

type GetTaskReply struct {
	MapTask     *queue.MapTask
	ReduceTask  *queue.ReduceTask
	ProcessDone bool
}

type CompleteMapTaskArgs struct {
	AttemptID    uuid.UUID
	CreatedFiles map[int]string
}

type CompleteReduceTask struct {
	AttemptID   uuid.UUID
	CreatedFile string
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
	completed, err := c.mq.CompleteTask(args.AttemptID)
	if err != nil {
		for _, tmp := range args.CreatedFiles {
			_ = os.Remove(tmp)
		}
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for partitionID, tmpPath := range args.CreatedFiles {
		finalPath := common.FinalMapOutPath(partitionID, completed.MapID)

		if err := os.Rename(tmpPath, finalPath); err != nil {
			_ = os.Remove(tmpPath)
			continue
		}

		c.intermediateFiles[partitionID] = append(
			c.intermediateFiles[partitionID],
			finalPath,
		)
	}

	return nil
}

func (c *Coordinator) CompleteReduceTask(args CompleteReduceTask, _ *struct{}) error {
	completed, err := c.rq.CompleteTask(args.AttemptID)
	if err != nil {
		os.Remove(args.CreatedFile)
		return nil
	}

	tmp := args.CreatedFile
	finalPath := common.FinalReduceOutPath(completed.ReduceID)

	if err := os.Rename(tmp, finalPath); err != nil {
		_ = os.Remove(tmp)
	}

	return nil
}

func (c *Coordinator) Done() bool {
	return c.mq.Done() && c.rq.Done()
}

func CoordinatorSock() string {
	s := "/var/tmp/5840-common-"
	s += strconv.Itoa(os.Getuid())
	return s
}
