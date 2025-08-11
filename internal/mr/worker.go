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
		task, err := w.getTask()
		if err != nil {
			log.Printf("Failed to get task %v", err)
		}

		if task.MapTask != nil {
			log.Printf("Successfully got map task %d", task.MapTask.ID)
			w.executeMapTask(task.MapTask)
		}

		if task.ReduceTask != nil {
			log.Printf("Successfully got reduce task %v", task.ReduceTask)
			w.executeReduceTask(task.ReduceTask)
		}
	}
}

func (w *Worker) getTask() (GetTaskReply, error) {
	args := struct{}{}
	reply := GetTaskReply{}

	if ok := w.call("Coordinator.GetTask", &args, &reply); !ok {
		return reply, fmt.Errorf("failed to call Coordinator.GetTask")
	}

	return reply, nil
}

func (w *Worker) executeMapTask(m *MapTask) {
	contents, err := os.ReadFile(m.Filename)
	if err != nil {
		log.Fatal(err)
	}

	keyValues := w.mapf(m.Filename, string(contents))

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
	for bucketID, bucket := range buckets {
		dir := fmt.Sprintf("out/intermediate/reduce-%d", bucketID)
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Printf("Failed to create directory %s: %v", dir, err)
		}

		filename := fmt.Sprintf("%s/map-%d", dir, m.ID)

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

	w.callCompleteMapTask(m.ID)
}

func (w *Worker) executeReduceTask(r *ReduceTask) {
	dir := fmt.Sprintf("out/intermediate/reduce-%d", r.ID)
	files, err := os.ReadDir(dir)
	if err != nil {
		log.Printf("Failed to read directory %s: %v", dir, err)
		return
	}

	kvMap := make(map[string][]string)

	for _, file := range files {
		fpath := fmt.Sprintf("%s/%s", dir, file.Name())
		f, err := os.Open(fpath)
		if err != nil {
			log.Printf("Failed to open file %s: %v", fpath, err)
			continue
		}

		var bucket map[string][]string
		if err := json.NewDecoder(f).Decode(&bucket); err != nil {
			log.Printf("Failed to decode JSON from %s: %v", fpath, err)
			f.Close()
			continue
		}
		f.Close()

		for k, vs := range bucket {
			kvMap[k] = append(kvMap[k], vs...)
		}
	}

	// Apply reduce function and write output
	outDir := "out/final"
	if err := os.MkdirAll(outDir, 0755); err != nil {
		log.Printf("Failed to create output directory %s: %v", outDir, err)
		return
	}

	outFile := fmt.Sprintf("%s/%d", outDir, r.ID)
	file, err := os.Create(outFile)
	if err != nil {
		log.Printf("Failed to create output file %s: %v", outFile, err)
		return
	}
	defer file.Close()

	for k, vs := range kvMap {
		output := w.reducef(k, vs)
		fmt.Fprintf(file, "%v %v\n", k, output)
	}

	w.callCompleteReduceTask(r.ID)
}

func (w *Worker) callCompleteMapTask(taskID int) error {
	args := CompleteTaskArgs{TaskID: taskID}
	reply := struct{}{}

	if ok := w.call("Coordinator.CompleteMapTask", &args, &reply); !ok {
		return fmt.Errorf("failed to call Coordinator.CompleteMapTask")
	}

	return nil
}

func (w *Worker) callCompleteReduceTask(taskID int) error {
	args := CompleteTaskArgs{TaskID: taskID}
	reply := struct{}{}

	if ok := w.call("Coordinator.CompleteReduceTask", &args, &reply); !ok {
		return fmt.Errorf("failed to call Coordinator.CompleteReduceTask")
	}

	return nil
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
