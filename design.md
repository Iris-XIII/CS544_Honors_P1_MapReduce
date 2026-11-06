# MapReduce Design Document

**Special Feature:** Straggler Detection and Speculative Execution

## 1. Client Interface

The client will use a **command-line interface (CLI)**.

Users will use command-line commands to:

- Upload or place starter data into shared storage.
- Specify the input path.
- Specify the output path.
- Provide a file containing user-defined `map` and `reduce` functions.
- Specify the number of map tasks.
- Specify the number of reduce tasks.
- Submit the MapReduce job to the boss node.

An example command may look like:

```bash
python client.py \
  --input /shared/input/data.csv \
  --output /shared/output/result \
  --job wordcount.py \
  --maps 4 \
  --reduces 2
```

The client will communicate with the boss through gRPC.

## 2. Implementation Language

The system will be implemented in **Python**.

## 3. System Communication

Communication between system components will use **gRPC**.

The main communication paths will be:

```text
Client
  |
  | gRPC
  v
Boss
  |
  | gRPC
  v
Workers
```

The client will submit jobs to the boss via gRPC. The boss will coordinate map and reduce task assignments with the workers and track their execution status.

Protocol Buffers will be used to define the RPC messages and service interfaces.

## 4. Shared Storage

The system will use a **shared volume mounted across the Docker containers**.

Because the system will be deployed on a single VM, a shared volume provides a convenient way for the boss and all workers to access the same files.

The shared storage may be organized as:

```text
/shared/
├── input/
├── intermediate/
└── output/
```

All workers will mount the same shared directory.

The input directory will contain the original input data, the intermediate directory will contain mapper output used by reducers, and the output directory will contain the final results.

## 5. Data Formats

### 5.1 RPC Messages

**Protocol Buffers**

Protocol Buffers will define structured messages exchanged through gRPC between the client, boss, and workers.

Examples include:

- Job submission requests
- Map task assignments
- Reduce task assignments
- Task completion responses
- Task failure responses

### 5.2 Input Data

**CSV or JSONL**

The system will initially support simple structured input formats such as CSV and JSONL.

Input files will be placed in shared storage before the job is submitted.

### 5.3 Intermediate Data

**JSONL**

Mapper output will be stored as key-value records using JSONL.

Example:

```jsonl
{"key": "apple", "value": 1}
{"key": "banana", "value": 1}
{"key": "apple", "value": 1}
```

Intermediate records will later be partitioned and grouped for reduce tasks.

### 5.4 Final Output

**Text or JSONL**

Reducer results will be written to the output directory in either plain-text or JSONL format.

For example:

```jsonl
{"key": "apple", "value": 2}
{"key": "banana", "value": 1}
```

## 6. Test Evaluation

### 6.1 Basic Correctness Test

Run a simple word-count job and compare the output against a known expected result.

### 6.2 Multiple Map/Reduce Counts

Run the same job with different configurations, such as `1 map / 1 reduce`, `4 maps / 2 reduces`, and `8 maps / 4 reduces`, and verify that the final output is identical.

### 6.3 Partitioning/Shuffle Test

Verify that all intermediate values for the same key are sent to the same reducer.

### 6.4 Multi-Worker Execution Test

Confirm that tasks are actually distributed across multiple worker containers rather than accidentally all being executed by one worker.

### 6.5 Edge-Case Tests

Test empty input, fewer records than map tasks, one reducer, and invalid or missing input paths.

### 6.6 User-Defined Function Test

Provide more than one MapReduce job, such as word count and sum/average, to verify that the system actually executes user-provided map/reduce functions rather than being hard-coded for one example.

## 7. Performance Evaluation

We will evaluate the performance of our MapReduce implementation by measuring job execution time, task distribution, and the effectiveness of speculative execution.

### 7.1 Baseline Performance

- Run the same MapReduce job using 1, 2, and 4 workers.
- Measure total job completion time for each configuration.
- Calculate speedup relative to single-worker execution.
- Repeat each experiment multiple times and report the average execution time.

**Metrics:**

- Total job execution time (seconds)
- Number of completed tasks per worker
- Speedup relative to single-worker execution

**Plot:** Number of workers vs. total job execution time.

### 7.2 Speculative Execution Evaluation

To evaluate our special feature, we will simulate straggler tasks by introducing artificial delays to selected worker tasks.

Compare two configurations:

1. **Without speculative execution:** The system waits for the delayed task to finish.
2. **With speculative execution:** The boss detects a straggler and launches a duplicate task on another available worker. The first successful attempt is accepted.

Both configurations will use the same dataset, number of workers, and map/reduce task settings.

**Metrics:**

- Total job completion time (seconds)
- Time taken to detect a straggler
- Number of speculative tasks launched
- Task execution time for original and duplicate attempts
- Percentage improvement in job completion time

**Plots:**

1. **Job completion time with vs. without speculative execution:** A bar chart comparing execution times under simulated straggler conditions.
2. **Straggler delay vs. job completion time:** A line plot showing how increasing artificial delays affect overall runtime with and without speculative execution.

### 7.3 Correctness and Overhead

Verify that speculative execution does not produce duplicate or incorrect outputs.

- Compare final outputs with and without speculative execution.
- Ensure that only one successful attempt per logical task is committed.
- Measure whether speculative execution introduces unnecessary overhead when no stragglers are present.

These experiments will help determine whether speculative execution improves performance under straggler conditions while preserving correctness.
