package coordinator

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/svaan1/map-reduce-go/internal/queue"
)

type Coordinator struct {
	mq *queue.MapQueue
	rq *queue.ReduceQueue

	files   []string
	nReduce int

	mu                sync.Mutex
	intermediateFiles map[int][]string
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mq: queue.NewMapQueue(),
		rq: queue.NewReduceQueue(),

		files:   files,
		nReduce: nReduce,

		intermediateFiles: make(map[int][]string),
	}

	c.addMapTasks()
	go c.addReduceTasks()
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

func (c *Coordinator) addMapTasks() {
	for i, file := range c.files {
		task := &queue.MapTask{
			MapID:    i,
			NReduce:  c.nReduce,
			Filename: file,
		}

		if err := c.mq.AddNewTask(task); err != nil {
			log.Fatal(err)
		}
	}
}

func (c *Coordinator) addReduceTasks() {
	for !c.mq.Done() {
		time.Sleep(10 * time.Millisecond)
	}

	for id, files := range c.intermediateFiles {
		task := &queue.ReduceTask{
			ReduceID: id,
			Files:    files,
		}

		if err := c.rq.AddNewTask(task); err != nil {
			log.Fatal(err)
		}
	}

	c.rq.Start()
}
