package worker

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/svaan1/map-reduce-go/internal/queue"
)

func (w *Worker) executeReduceTask(r *queue.ReduceTask) {
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
