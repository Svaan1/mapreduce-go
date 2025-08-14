#!/usr/bin/env bash

#
# map-reduce tests
#
# This script has been reorganized to use functions for each test case,
# improving modularity and readability.
#

# Exit script immediately if a command exits with a non-zero status.
set -e
# Treat unset variables as an error when substituting.
set -u
# Pipelines return the exit status of the last command to exit with a non-zero status.
set -o pipefail

# --- Configuration ---

# Set a race flag for Go's race detector. Can be overridden.
# Example: RACE=-race ./test-mr.sh
RACE=${RACE:-}

# Check for Go version compatibility issues on macOS that might affect the race detector.
if [[ "$OSTYPE" = "darwin"* ]]; then
  if go version | grep -q 'go1.17.[0-5]'; then
    if [[ -n "$RACE" ]]; then
      echo '*** Turning off -race due to potential instability on this macOS/Go version.'
      RACE=""
    fi
  fi
fi

# Determine the correct timeout command.
if command -v gtimeout &> /dev/null; then
  TIMEOUT_CMD="gtimeout"
elif command -v timeout &> /dev/null; then
  TIMEOUT_CMD="timeout"
else
  echo '*** WARNING: Cannot find timeout command; proceeding without timeouts.'
  TIMEOUT_CMD=""
fi

# Define timeout durations.
TIMEOUT_NORMAL=""
TIMEOUT_LONG=""
if [[ -n "$TIMEOUT_CMD" ]]; then
  TIMEOUT_NORMAL="$TIMEOUT_CMD -k 2s 45s"
  TIMEOUT_LONG="$TIMEOUT_CMD -k 2s 120s"
fi

# Paths and commands
CMD_DIR="../cmd"
PLUGINS_DIR="../plugins"
DATA_DIR="../data"
SEQ_CMD="$CMD_DIR/sequential/main"
COORD_CMD="$CMD_DIR/coordinator/main"
WORKER_CMD="$CMD_DIR/worker/main"
TEST_TMP_DIR="tmp"
FINAL_OUT_DIR="out/final"

# Global state for tracking test failures.
FAILED_ANY=0
# Argument to control output verbosity.
IS_QUIET=${1:-}

# --- Helper Functions ---

# Silences command output if the "quiet" argument is passed to the script.
maybe_quiet() {
  if [[ "$IS_QUIET" == "quiet" ]]; then
    "$@" &> /dev/null
  else
    "$@"
  fi
}

# Executes a command with a timeout, if available.
run_with_timeout() {
  local timeout_duration_type=$1
  shift
  local cmd_to_run=("$@")

  local timeout_val=""
  if [[ "$timeout_duration_type" == "long" ]]; then
    timeout_val="$TIMEOUT_LONG"
  else
    timeout_val="$TIMEOUT_NORMAL"
  fi

  if [[ -n "$timeout_val" ]]; then
    maybe_quiet $timeout_val "${cmd_to_run[@]}"
  else
    maybe_quiet "${cmd_to_run[@]}"
  fi
}

# Sets up a clean directory for a test run.
setup_test_dir() {
  echo "*** Setting up test environment in '$TEST_TMP_DIR'..."
  rm -rf "$TEST_TMP_DIR"
  mkdir -p "$TEST_TMP_DIR"
  cd "$TEST_TMP_DIR"
  rm -f mr-*
}

# Prints the result of a test and updates the failure flag.
report_test_result() {
  local test_name=$1
  local status=$2 # "PASS" or "FAIL"
  local message=${3:-} # Default to empty string if not provided

  echo "--- $test_name test: $status"
  if [[ "$status" == "FAIL" ]]; then
    echo "    Reason: $message"
    FAILED_ANY=1
  fi
}

# Compares the MapReduce output with the correct sequential output.
check_output() {
  local test_name=$1
  local correct_file=$2
  local generated_file="mr-output-all.txt"

  # Create the directory if it doesn't exist to avoid errors on sort
  mkdir -p "$FINAL_OUT_DIR"
  # Use find to avoid errors if no files match the glob
  find "$FINAL_OUT_DIR" -type f -name '*' -print0 | xargs -0 sort | grep . > "$generated_file"

  if cmp "$generated_file" "$correct_file"; then
    report_test_result "$test_name" "PASS"
  else
    report_test_result "$test_name" "FAIL" "Output is not the same as $correct_file."
  fi
}

# --- Test Cases ---

test_word_count() {
  echo "*** Starting wc test"
  rm -f mr-* "$FINAL_OUT_DIR"/*
  local correct_file="mr-correct-wc.txt"

  # Generate correct output sequentially.
  "$SEQ_CMD" "$PLUGINS_DIR/wc/wc.so" "$DATA_DIR"/pg*txt
  sort mr-out-0 > "$correct_file"
  rm -f "$FINAL_OUT_DIR"/*

  # Run MapReduce job.
  run_with_timeout normal "$COORD_CMD" "$DATA_DIR"/pg*txt &
  local coord_pid=$!
  sleep 1

  for i in {1..3}; do
    run_with_timeout normal "$WORKER_CMD" "$PLUGINS_DIR/wc/wc.so" &
  done

  wait $coord_pid
  wait # Wait for all workers to finish.

  check_output "wc" "$correct_file"
}

test_indexer() {
  echo "*** Starting indexer test"
  rm -f mr-* "$FINAL_OUT_DIR"/*
  local correct_file="mr-correct-indexer.txt"

  # Generate correct output sequentially.
  "$SEQ_CMD" "$PLUGINS_DIR/indexer/indexer.so" "$DATA_DIR"/pg*txt
  sort mr-out-0 > "$correct_file"
  rm -f "$FINAL_OUT_DIR"/*

  # Run MapReduce job.
  run_with_timeout normal "$COORD_CMD" "$DATA_DIR"/pg*txt &
  sleep 1

  for i in {1..2}; do
    run_with_timeout normal "$WORKER_CMD" "$PLUGINS_DIR/indexer/indexer.so" &
  done

  wait # Wait for coordinator and workers.
  check_output "indexer" "$correct_file"
}

test_map_parallelism() {
  echo "*** Starting map parallelism test"
  rm -f mr-* "$FINAL_OUT_DIR"/*

  run_with_timeout normal "$COORD_CMD" "$DATA_DIR"/pg*txt &
  sleep 1

  run_with_timeout normal "$WORKER_CMD" "$PLUGINS_DIR/mtiming/mtiming.so" &
  run_with_timeout normal "$WORKER_CMD" "$PLUGINS_DIR/mtiming/mtiming.so" &
  wait

  local num_workers=$(cat $FINAL_OUT_DIR/* | grep '^times-' | wc -l | tr -d ' ')
  if [[ "$num_workers" -ne 2 ]]; then
    report_test_result "map parallelism" "FAIL" "Saw $num_workers workers rather than 2."
    return
  fi

  if grep -q '^parallel.* 2' $FINAL_OUT_DIR/*; then
    report_test_result "map parallelism" "PASS"
  else
    report_test_result "map parallelism" "FAIL" "Map workers did not run in parallel."
  fi
}

test_reduce_parallelism() {
    echo "*** Starting reduce parallelism test"
    rm -f mr-* "$FINAL_OUT_DIR"/*

    run_with_timeout normal "$COORD_CMD" "$DATA_DIR"/pg*txt &
    sleep 1

    run_with_timeout normal "$WORKER_CMD" "$PLUGINS_DIR/rtiming/rtiming.so" &
    run_with_timeout normal "$WORKER_CMD" "$PLUGINS_DIR/rtiming/rtiming.so" &
    wait

    local parallel_reduces=$(cat $FINAL_OUT_DIR/* | grep '^[a-z] 2' | wc -l | tr -d ' ')
    if [[ "$parallel_reduces" -ge 2 ]]; then
        report_test_result "reduce parallelism" "PASS"
    else
        report_test_result "reduce parallelism" "FAIL" "Too few parallel reduces detected."
    fi
}

test_job_count() {
    echo "*** Starting job count test"
    rm -f mr-* "$FINAL_OUT_DIR"/*

    run_with_timeout normal "$COORD_CMD" "$DATA_DIR"/pg*txt &
    sleep 1

    for i in {1..4}; do
        run_with_timeout normal "$WORKER_CMD" "$PLUGINS_DIR/jobcount/jobcount.so" &
    done
    wait

    local job_count=$(cat $FINAL_OUT_DIR/* | awk '{print $2}')
    if [[ "$job_count" -eq 8 ]]; then
        report_test_result "job count" "PASS"
    else
        report_test_result "job count" "FAIL" "Map jobs ran $job_count times instead of 8."
    fi
}

test_early_exit() {
    echo "*** Starting early exit test"
    rm -f mr-* "$FINAL_OUT_DIR"/*
    local done_file="anydone.$$"
    rm -f "$done_file"

    (run_with_timeout normal "$COORD_CMD" "$DATA_DIR"/pg*txt; touch "$done_file") &
    sleep 1

    for i in {1..3}; do
        (run_with_timeout normal "$WORKER_CMD" "$PLUGINS_DIR/early_exit/early_exit.so"; touch "$done_file") &
    done

    # Wait for the first process (coordinator or any worker) to exit.
    while [[ ! -f "$done_file" ]]; do
        sleep 0.1
    done
    rm -f "$done_file"

    # At this point, the job should be fully complete. Capture the output.
    local initial_output="mr-output-initial.txt"
    sort $FINAL_OUT_DIR/* | grep . > "$initial_output"

    # Wait for all remaining processes to finish.
    wait

    # Capture the final output and compare.
    local final_output="mr-output-final.txt"
    sort $FINAL_OUT_DIR/* | grep . > "$final_output"

    if cmp "$initial_output" "$final_output"; then
        report_test_result "early exit" "PASS"
    else
        report_test_result "early exit" "FAIL" "Output changed after the first process exited."
    fi
}

test_crash_tolerance() {
    echo "*** Starting crash test"
    rm -f mr-* "$FINAL_OUT_DIR"/*
    local correct_file="mr-correct-crash.txt"

    # Generate correct output with a non-crashing plugin.
    "$SEQ_CMD" "$PLUGINS_DIR/nocrash/nocrash.so" "$DATA_DIR"/pg*txt
    sort mr-out-0 > "$correct_file"
    rm -f "$FINAL_OUT_DIR"/*

    local done_file="mr-done"
    rm -f "$done_file"
    (run_with_timeout long "$COORD_CMD" "$DATA_DIR"/pg*txt; touch "$done_file") &
    sleep 1

    # This socket name must match the one in rpc.go
    local sockname="/var/tmp/5840-mr-$(id -u)"

    # Continuously start new workers to replace crashed ones.
    while [[ -e "$sockname" && ! -f "$done_file" ]]; do
        run_with_timeout long "$WORKER_CMD" "$PLUGINS_DIR/crash/crash.so" || true # Ignore exit code
        sleep 0.1
    done &
    local crash_loop_pid=$!

    wait # Wait for coordinator to finish.
    kill "$crash_loop_pid" 2>/dev/null || true # Kill the worker-starting loop.
    wait # Final cleanup.

    rm -f "$sockname"
    check_output "crash" "$correct_file"
}

# --- Main Execution ---

main() {
  # Ensure software is freshly built.
  echo "*** Building project..."
  if ! make build; then
    echo "Build failed. Aborting tests."
    exit 1
  fi

  setup_test_dir

  # Run all tests
  test_word_count
  test_indexer
  test_map_parallelism
  test_reduce_parallelism
  test_job_count
  test_early_exit
  test_crash_tolerance

  # Final summary
  echo ""
  echo "---"
  if [ $FAILED_ANY -eq 0 ]; then
    echo "*** PASSED ALL TESTS ***"
    cd ..
    rm -rf "$TEST_TMP_DIR"
    exit 0
  else
    echo "*** FAILED SOME TESTS ***"
    echo "Test artifacts are in the '$TEST_TMP_DIR' directory."
    exit 1
  fi
}

# Run the main function
main "$@"
