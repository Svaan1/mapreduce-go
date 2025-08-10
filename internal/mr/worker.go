package mr

import (
	"encoding/json"
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
		task, err := w.GetTask()
		if err != nil {
			log.Printf("Failed to get task %v", err)
		}

		if task.MapTask != nil {
			log.Printf("Successfully got map task %d", task.MapTask.ID)

			w.ExecuteMapTask(task.MapTask)
		}

		if task.ReduceTask != nil {
			log.Printf("Successfully got reduce task %v", task.ReduceTask)

		}

	}
}

func (w *Worker) ExecuteMapTask(m *MapTask) {
	keyValues := w.mapf(m.Filename, m.Contents)

	// TODO: create temporary file instead and rename it on completion

	// Separate all key values into `nReduce` buckets
	buckets := make(map[int]map[string][]string)
	for _, kv := range keyValues {
		reduceID := ihash(kv.Key) % m.NReduce

		bucket, exists := buckets[reduceID]
		if !exists {
			bucket = make(map[string][]string)
		}

		values, exists := bucket[kv.Key]
		if !exists {
			values = []string{}
		}

		values = append(values, kv.Value)
		bucket[kv.Key] = values
		buckets[reduceID] = bucket
	}

	// Write all buckets into files
	for id, bucket := range buckets {
		filename := fmt.Sprintf("out/mr-%d-%d", m.ID, id)

		file, err := os.Create(filename)
		if err != nil {
			log.Printf("Failed to create file %s: %v", filename, err)
			continue
		}

		enc := json.NewEncoder(file)
		enc.SetIndent("", "  ")

		if err := enc.Encode(bucket); err != nil {
			log.Printf("Failed to write JSON: %v", err)
		}

		file.Close()
	}

	w.CompleteMapTask(m.ID)
}

func (w *Worker) GetTask() (GetTaskReply, error) {
	args := struct{}{}
	reply := GetTaskReply{}

	if ok := w.call("Coordinator.GetTask", &args, &reply); !ok {
		return reply, fmt.Errorf("failed to call Coordinator.GetTask")
	}

	return reply, nil
}

func (w *Worker) CompleteMapTask(taskID int) error {
	args := CompleteTaskArgs{TaskID: taskID}
	reply := struct{}{}

	if ok := w.call("Coordinator.CompleteTask", &args, &reply); !ok {
		return fmt.Errorf("failed to call Coordinator.CompleteTask")
	}

	return nil
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
