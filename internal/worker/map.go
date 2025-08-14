package worker

import (
	"encoding/json"
	"log"
	"os"

	"github.com/svaan1/map-reduce-go/internal/common"
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
		partitionID := ihash(kv.Key) % m.NReduce

		bucket, exists := buckets[partitionID]
		if !exists {
			bucket = make(map[string][]string)
		}

		values, exists := bucket[kv.Key]
		if !exists {
			values = []string{}
		}

		values = append(values, kv.Value)
		bucket[kv.Key] = values
		buckets[partitionID] = bucket
	}

	// Write all buckets into files
	intermediateFiles := make(map[int]string)
	for partitionID, bucket := range buckets {
		dir := common.TempIntermediateDir(partitionID)
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Printf("Failed to create directory %s: %v", dir, err)
		}

		filename := common.TempMapOutPath(partitionID, m.MapID, m.AttemptID)
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

		intermediateFiles[partitionID] = filename
		file.Close()
	}

	w.callCompleteMapTask(m.AttemptID, intermediateFiles)
}
