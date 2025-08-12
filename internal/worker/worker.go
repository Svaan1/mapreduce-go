package worker

import (
	"hash/fnv"
	"log"
	"net/rpc"
	"time"

	"github.com/svaan1/map-reduce-go/internal/mr"
)

type Worker struct {
	client  *rpc.Client
	mapf    func(string, string) []mr.KeyValue
	reducef func(string, []string) string
}

func NewWorker(mapf func(string, string) []mr.KeyValue, reducef func(string, []string) string) Worker {
	w := Worker{
		client:  nil,
		mapf:    mapf,
		reducef: reducef,
	}

	return w
}

func (w *Worker) Work() {
	w.connect()
	defer w.client.Close()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for range ticker.C {
		task, err := w.callGetTask()
		if err != nil {
			log.Fatalf("Failed to get task %v", err)
		}

		if task.ProcessDone {
			return
		}

		if task.MapTask != nil {
			log.Printf("Successfully got map task %d", task.MapTask.ID)
			w.executeMapTask(task.MapTask)
		}

		if task.ReduceTask != nil {
			log.Printf("Successfully got reduce task %d", task.ReduceTask.ID)
			w.executeReduceTask(task.ReduceTask)
		}
	}
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
