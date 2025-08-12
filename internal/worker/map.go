package worker

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/svaan1/map-reduce-go/internal/queue"
)

func (w *Worker) executeMapTask(m *queue.MapTask) {
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
	intermediateFiles := make(map[int]string)
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

		intermediateFiles[bucketID] = filename

		file.Close()
	}

	w.callCompleteMapTask(m.ID, intermediateFiles)
}
