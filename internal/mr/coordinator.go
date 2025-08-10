package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	MQ *MapQueue
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MQ: &MapQueue{},
	}

	id := 1

	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			log.Fatal(err)
		}

		task := &MapTask{
			ID:       id,
			NReduce:  nReduce,
			Filename: file,
			Contents: string(data),
		}

		if err = c.MQ.AddNewTask(task); err != nil {
			log.Fatal(err)
		}

		id++
	}

	c.server()
	return &c
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}
