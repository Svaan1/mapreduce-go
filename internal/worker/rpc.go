package worker

import (
	"fmt"
	"log"
	"net/rpc"

	"github.com/svaan1/map-reduce-go/internal/coordinator"
)

func (w *Worker) callGetTask() (coordinator.GetTaskReply, error) {
	args := struct{}{}
	reply := coordinator.GetTaskReply{}

	if ok := w.call("Coordinator.GetTask", &args, &reply); !ok {
		return reply, fmt.Errorf("failed to call Coordinator.GetTask")
	}

	return reply, nil
}

func (w *Worker) callCompleteMapTask(taskID int, createdFiles map[int]string) error {
	args := coordinator.CompleteMapTaskArgs{TaskID: taskID, CreatedFiles: createdFiles}
	reply := struct{}{}

	if ok := w.call("Coordinator.CompleteMapTask", &args, &reply); !ok {
		return fmt.Errorf("failed to call Coordinator.CompleteMapTask")
	}

	return nil
}

func (w *Worker) callCompleteReduceTask(taskID int) error {
	args := coordinator.CompleteReduceTask{TaskID: taskID}
	reply := struct{}{}

	if ok := w.call("Coordinator.CompleteReduceTask", &args, &reply); !ok {
		return fmt.Errorf("failed to call Coordinator.CompleteReduceTask")
	}

	return nil
}

func (w *Worker) connect() {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinator.CoordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}

	w.client = c
}

func (w *Worker) call(rpcname string, args interface{}, reply interface{}) bool {
	err := w.client.Call(rpcname, args, reply)
	if err != nil {
		fmt.Println(err)
		return false
	}

	return true
}
