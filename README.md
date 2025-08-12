# Map-Reduce (Go)

An educational MapReduce exercise from the MIT 6.5840 course. Implemented in Go with a simple coordinator/worker design, pluggable Map/Reduce applications, a sequential runner for ground truth, and a test suite that validates correctness, parallelism, and crash tolerance.

## Overview

- Coordinator assigns map and reduce tasks over a Unix domain socket using Go net/rpc over HTTP.
- Workers dynamically load a plugin that defines `Map` and `Reduce` functions and execute tasks.
- Intermediate map outputs are written to per-reducer JSON files; reducers merge and write final output files.
- A sequential runner computes a reference output to compare with the distributed run output.
- Shell tests validate word count, indexing, parallelism, early-exit behavior, job re-execution counts, and crash tolerance.

## Repository layout

```
cmd/
  coordinator/   # coordinator binary
  worker/        # worker binary (loads plugins)
  sequential/    # sequential reference runner
internal/
  coordinator/   # RPC types and coordinator logic
  worker/        # worker RPC + map/reduce execution
  queue/         # in-memory task queues with timeout-based requeue
  mr/            # KeyValue type shared with plugins
plugins/         # example MapReduce applications (built as .so plugins)
data/            # sample input texts
Makefile         # build all commands and plugins
README.md        # this file
go.mod           # module definition
test.sh          # single-run test suite
test-many.sh     # repeat test suite N times
```

## Requirements

- Go 1.21+ recommended (ensure your local Go version matches `go.mod`; update if needed).
- Linux/macOS. Tests assume standard Unix tooling (`timeout` or `gtimeout`).

## Build

```
make build
```

Artifacts:

- Binaries: `cmd/coordinator/main`, `cmd/worker/main`, `cmd/sequential/main`
- Plugins: `plugins/*/*.so`

## Run a job (example: word count)

In one terminal (coordinator):

```
./cmd/coordinator/main data/pg*txt
```

In one or more terminals (workers):

```
./cmd/worker/main plugins/wc/wc.so
```

When the job completes, final outputs are in:

```
out/final/<reduceID>
```

Each line is `key value`.

## Sequential reference

Compute a reference output with the same plugin:

```
./cmd/sequential/main plugins/wc/wc.so data/pg*txt
# Produces: mr-out-0
```

## Test suite

Run all tests (builds first):

```
./test.sh            # add "quiet" to reduce output
```

Run the suite repeatedly (useful for catching flakiness):

```
./test-many.sh 5
```

Tests include:

- Word count and indexer correctness (match sequential output)
- Map and reduce parallelism
- Job re-execution counting
- Early exit behavior (output stability)
- Crash tolerance (workers that crash or stall)

## Plugin API

Plugins must be built with `-buildmode=plugin` and export:

```
func Map(filename string, contents string) []mr.KeyValue
func Reduce(key string, values []string) string
```

Build a plugin (example):

```
(cd plugins/wc && go build -buildmode=plugin wc.go)
```

Workers load the `.so` at runtime.

## How it works (high level)

- Coordinator
  - Creates one map task per input file and tracks task state in an in-memory queue.
  - After all map tasks are done, creates reduce tasks (one per partition that has data) and tracks their completion.
  - Exposes RPCs for workers to fetch a task and report completion.
- Worker
  - Polls the coordinator for work.
  - Map: reads the file, applies `Map`, partitions by `ihash(key)%NReduce`, and writes JSON buckets to `out/intermediate/reduce-<id>/map-<mapID>`.
  - Reduce: reads all intermediate buckets for its partition, aggregates values by key, applies `Reduce`, and writes `out/final/<reduceID>`.
- Queues
  - Map/Reduce queues requeue tasks after a timeout to tolerate crashes or stalled workers.

## Configuration knobs

- Reduce partitions: currently fixed to 10 in `cmd/coordinator/main.go` (change as needed).
- Task timeout: `internal/queue/*q.go` (`taskTimeout`).

## Known limitations (intentional for learning)

- At-least-once execution: tasks can be re-run after timeout; user functions should be idempotent. Late completions may still need guarding at the coordinator if you harden semantics.
- Intermediate/final file writes are not atomic; a crash mid-write can leave partial files that later reads must handle.
- Workers discover reduce inputs by scanning the filesystem; coordinator also tracks produced files—this duality can be unified.
- Simple polling (1s tick) for task fetching; no backoff or long-polling.
- Minimal logging/observability; structured logs would aid debugging.
- `go.mod` Go version may need alignment with your local toolchain.

## Roadmap ideas

- Attempt IDs and acceptance rules end-to-end; ignore late/stale completions.
- Atomic output publication (temp file + fsync + rename) and cleanup.
- Unify the contract for reduce inputs (either coordinator-provided lists or pure discovery).
- Replace fixed polling with long-poll or push.
- Structured logging, metrics, and richer test assertions.
- Configurable NReduce, timeouts, and output directories.

## Inspiration

Inspired by the MapReduce model and built for hands-on learning with Go’s RPC, plugins, and concurrency primitives.
