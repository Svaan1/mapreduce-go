package worker

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/svaan1/map-reduce-go/internal/common"
	"github.com/svaan1/map-reduce-go/internal/queue"
)

func (w *Worker) executeReduceTask(r *queue.ReduceTask) {
	kvMap := make(map[string][]string)

	for _, file := range r.Files {
		f, err := os.Open(file)
		if err != nil {
			log.Printf("Failed to open file %s: %v", file, err)
			continue
		}

		var bucket map[string][]string
		if err := json.NewDecoder(f).Decode(&bucket); err != nil {
			log.Printf("Failed to decode JSON from %s: %v", file, err)
			f.Close()
			continue
		}
		f.Close()

		for k, vs := range bucket {
			kvMap[k] = append(kvMap[k], vs...)
		}
	}

	outDir := common.FinalDir()
	if err := os.MkdirAll(outDir, 0755); err != nil {
		log.Printf("Failed to create output directory %s: %v", outDir, err)
		return
	}

	outFile := common.TempReduceOutPath(r.ReduceID, r.AttemptID)
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

	w.callCompleteReduceTask(r.AttemptID, outFile)
}
