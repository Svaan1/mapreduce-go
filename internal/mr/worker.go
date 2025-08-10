package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

type Worker struct {
	client  *rpc.Client
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

func NewWorker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) Worker {
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
		task, err := w.CallGetTask()
		if err != nil {
			log.Printf("Failed to get task %v", err)
		}

		if task.MapTask != nil {
			log.Printf("Successfully got task %d", task.MapTask.ID)

			keyValues := w.mapf(task.MapTask.Filename, task.MapTask.Contents)

			// Separate all key values into `nReduce` values
			intermediateKeys := make(map[int][]KeyValue)
			for _, kv := range keyValues {
				reduceID := ihash(kv.Key) % task.MapTask.NReduce

				v, exists := intermediateKeys[reduceID]
				if !exists {
					intermediateKeys[reduceID] = []KeyValue{kv}
					continue
				}

				intermediateKeys[reduceID] = append(v, kv)
			}

			// Write all buckets into files
			for reduceID, value := range intermediateKeys {
				filename := fmt.Sprintf("mr-%d-%d", task.MapTask.ID, reduceID)

				file, err := os.Create(filename)
				if err != nil {
					log.Printf("Failed to create file %s: %v", filename, err)
					continue
				}

				for _, kv := range value {
					_, err := fmt.Fprintf(file, "%d - %d, %s, %s\n", task.MapTask.ID, reduceID, kv.Key, kv.Value)
					if err != nil {
						log.Printf("Failed to write to file %s: %v", filename, err)
						break
					}
				}

				file.Close()
			}

		}

	}
}

func (w *Worker) CallGetTask() (GetTaskReply, error) {
	args := struct{}{}
	reply := GetTaskReply{}

	if ok := w.call("Coordinator.GetTask", &args, &reply); !ok {
		return reply, fmt.Errorf("failed to call Coordinator.GetTask")
	}

	return reply, nil
}

func (w *Worker) connect() {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
