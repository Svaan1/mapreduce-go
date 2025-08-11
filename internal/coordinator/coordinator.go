package coordinator

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"

	"github.com/svaan1/map-reduce-go/internal/queue"
)

type Coordinator struct {
	mq *queue.MapQueue
	rq *queue.ReduceQueue
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mq: &queue.MapQueue{},
		rq: &queue.ReduceQueue{},
	}

	// Add map tasks
	for i, file := range files {
		task := &queue.MapTask{
			ID:       i,
			NReduce:  nReduce,
			Filename: file,
		}

		if err := c.mq.AddNewTask(task); err != nil {
			log.Fatal(err)
		}
	}

	// Add reduce tasks
	for i := range nReduce {
		task := &queue.ReduceTask{
			ID: i,
		}

		if err := c.rq.AddNewTask(task); err != nil {
			log.Fatal(err)
		}
	}

	c.server()
	return &c
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := CoordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}
