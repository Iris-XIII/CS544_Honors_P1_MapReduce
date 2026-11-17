# CS544 Honors MapReduce

A Python/gRPC MapReduce implementation in progress. See [design.md](design.md)
for the system design and planned speculative execution feature.

## Current scope

The Boss supports job registration, status tracking, and baseline push scheduling.
With configured Workers, it runs one job at a time, dispatches maps, waits for all
maps, then dispatches reducers and publishes their outputs. Without configured
Workers, it runs in API-only mode and jobs stay `JOB_QUEUED`.

The Worker executes map/shuffle/reduce tasks asynchronously through gRPC. The
scheduler supplies explicit reducer input files and publishes completed outputs.
Self-contained tests exercise the full Boss/Worker pipeline. The client CLI,
speculative execution, and Docker Compose deployment remain to be implemented.
Job records live only in memory and are lost when the Boss restarts.

## Setup

Run commands from the repository root using Python 3.12:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
export PYTHONPATH="$PWD/proto:$PWD"
```

On Debian/Ubuntu, creating a virtual environment requires the OS's `python3-venv`
package. The pinned dependencies match the existing Worker generated code.
Generated files are committed, so regeneration is only needed after editing a
`.proto` file:

```bash
python -m grpc_tools.protoc -Iproto --python_out=proto --grpc_python_out=proto proto/boss.proto
# Only when changing the Worker protocol:
python -m grpc_tools.protoc -Iproto --python_out=proto --grpc_python_out=proto proto/mapreduce.proto
```

The generation command follows the [gRPC Python quick start](https://grpc.io/docs/languages/python/quickstart/).
Edit the `.proto` source, not the generated `*_pb2.py` or `*_pb2_grpc.py` files.

## Start the Boss

```bash
python -m boss.server
# Optional alternate bind address:
python -m boss.server --address 127.0.0.1:50050
```

The default address is `[::]:50050`; the existing Worker uses port `50051`.
Stop with Ctrl+C. These development servers use insecure gRPC.

To enable scheduling, supply Worker addresses and a workspace visible at the same
absolute path to the Boss and every Worker:

```bash
python -m boss.server \
  --workers worker1:50051 worker2:50051 worker3:50051 worker4:50051 \
  --work-dir /shared/intermediate \
  --poll-interval 0.2 --rpc-timeout 3 --task-timeout 300
```

Those hostnames are illustrative container addresses; Docker Compose is not set
up yet. With local Workers use their actual host/port addresses. Each address must
identify a different Worker. Start each Worker with `python -m worker.worker`;
`WORKER_PORT` controls its port and `SHARED_ROOT` controls its shared-storage root
(defaults: `50051` and `/shared`). The
[scheduler/Worker contract](docs/scheduler-worker-contract.md) documents the
interface and shared paths.

## Try submission and status lookup

Start the Boss **without `--workers`** for this registration-only example.
In another terminal, activate the environment and set `PYTHONPATH` as above.
This example creates temporary local files standing in for shared storage. It
registers a word-count job and queries its status; it does not execute the job.

```bash
python - <<'PY'
from pathlib import Path
import tempfile
import grpc
import boss_pb2
import boss_pb2_grpc

with tempfile.TemporaryDirectory() as directory:
    root = Path(directory)
    (root / "input.txt").write_text("hello world\nhello mapreduce\n")
    (root / "wordcount.py").write_text(
        "def map(key, value):\n"
        "    for word in value.split():\n"
        "        yield word, 1\n\n"
        "def reduce(key, values):\n"
        "    return key, sum(values)\n"
    )
    with grpc.insecure_channel("localhost:50050") as channel:
        stub = boss_pb2_grpc.BossServiceStub(channel)
        submitted = stub.SubmitJob(boss_pb2.SubmitJobRequest(
            input_path=str(root / "input.txt"),
            output_path=str(root / "output"),
            job_file=str(root / "wordcount.py"),
            num_maps=4,
            num_reducers=2,
        ), timeout=5)
        status = stub.GetJobStatus(
            boss_pb2.JobStatusRequest(job_id=submitted.job_id), timeout=5
        )
        print(status)
PY
```

Expect a unique `job_id`, state `JOB_QUEUED`, four maps, and two reducers.
Completed task counters are zero (protobuf's text display omits zero fields).
The returned output path is the requested destination, not proof of completed
results. Keep real job files and input data available until execution finishes;
the temporary example above is only for registration testing.

### API contract

- `SubmitJob` takes the five assignment settings: input file, output directory,
  Python job file, map count, and reducer count. Counts must be positive.
- Paths must be absolute **as seen by the Boss and Workers**, e.g.
  `/shared/input/data.txt` inside containers. A host-only path will not work when
  the Boss runs in Docker. Input and job files must exist and be readable.
- Output may be an existing directory or a directory to create later; it cannot
  be a file or have a file as a parent. Submission does not write to it or check
  future write permissions. When scheduling starts, output must be empty and must
  not overlap the job workspace. Use a distinct output directory for each job.
- Submission does not execute user code or verify that its map/reduce functions
  are valid. Execution errors will be handled by the Worker/scheduler later.
- Invalid submissions and empty status-query IDs return gRPC `INVALID_ARGUMENT`.
  An unknown job ID returns `NOT_FOUND`.
- Each successful submission creates a new job, even if the settings are
  identical. Submission retries are not deduplicated.
- Job states are `JOB_QUEUED`, `JOB_RUNNING`, `JOB_COMPLETED`, and `JOB_FAILED`.
  A configured scheduler advances these states; API-only mode keeps jobs queued.
  `JOB_UNKNOWN` is the protobuf default; missing jobs produce `NOT_FOUND` instead.

## How the implementation fits together

1. **`proto/boss.proto`: the contract.** Defines the two client-to-Boss RPCs and
   their messages. It is separate from Iris's Worker protocol to allow independent
   changes. Job enum values use a `JOB_` prefix to avoid Worker enum name clashes.
2. **`boss/jobs.py`: the records.** `JobSpec` holds the five submitted settings.
   `JobRecord` adds the ID, state, counters, timestamp, and error field. `JobStore`
   protects dictionary access with a lock. Records and their specs are immutable,
   so callers cannot accidentally modify shared state after a lookup.
3. **`boss/server.py`: the RPC handlers.** `SubmitJob` validates settings and
   creates a record. `GetJobStatus` converts a record into a protobuf reply.
   `create_server` connects the handlers to gRPC; `main` starts the server.
4. **`tests/test_boss_rpc.py`: the behavior checks.** Tests start a real server on
   a free local port and make RPC calls. They cover submission, polling, invalid
   inputs, unknown IDs, no user-code execution during registration, immutable
   records, and simultaneous submissions.
5. **`boss/scheduler.py`: scheduling.** One background thread claims jobs from a
   FIFO queue. Each phase has its own task queue and tracks at most one active task
   per Worker. It polls statuses, records completions, and starts reduces only
   after all logical maps succeed. Progress counts each logical task once.
6. **`tests/test_scheduler.py`: scheduling checks.** Controlled gRPC Workers test
   task distribution, line ranges, the map/reduce barrier, FIFO job order, output
   partition selection, progress, publication, failures, timeouts, and shutdown. They
   do not test the real Worker execution algorithms.
7. **`tests/test_boss_worker_integration.py`: real execution.** These tests start
   their own Boss and four Worker servers on available ports.
   They check word count across task counts, empty input, a second user-defined
   job, and exclusion of unselected attempt outputs.

## Baseline scheduler behavior

- Jobs execute in registration order, one job at a time. Tasks within a phase run
  concurrently across Workers. Every task uses `attempt_id=0`.
- The Boss counts UTF-8 text lines and assigns balanced `[start_line, end_line)`
  ranges. Empty input or fewer lines than maps still produces the requested number
  of map tasks, including empty ranges. Input files must stay unchanged throughout
  the job. Multiline CSV records are not supported by this line-based partitioning.
- After all maps complete, the Boss selects each reducer's partition file from
  every successful map attempt. It checks all files exist before dispatching any
  reducer, then supplies the list through `ReduceTaskRequest.input_paths`.
  `intermediate_path` is left empty; no manifest needs to be parsed by the Worker.
- Reducers write separate attempt files in the job workspace. After all succeed,
  the Boss copies results into `output_path/part-00000.jsonl`, etc., using an atomic
  rename per file. Only `JOB_COMPLETED` means the complete result is available.
  A filesystem error during publication can leave partial files; the job fails
  and those files must not be treated as a complete result.
- Rejection, unknown attempts, failed tasks, missing outputs, RPC errors, or task
  timeouts fail the job. No retries or speculative attempts are launched.
- `--rpc-timeout` limits each RPC. `--task-timeout` measures wall-clock time since
  dispatch, including time pending on the Worker; it does not trust Worker timing.
- On job failure, Workers still running or with uncertain task status are disabled
  for the rest of this Boss process. There is no cancellation RPC, so immediately
  reusing them could overlap tasks. Workers confirmed terminal can run later jobs.
  If none remain usable, later jobs fail with a clear error. To reset, stop/restart
  affected Workers before restarting the Boss. Workspaces are retained for review;
  the scheduler does not delete them automatically.
- Ctrl+C stops the Boss and scheduler. The active job is marked failed in memory;
  remote tasks cannot be cancelled through the current protocol. Restart loses
  all job records, including queued jobs.

## Tests

```bash
python -m unittest discover -s tests -p 'test_*.py' -v
```

The GitHub Actions workflow runs these tests on PRs and checks that generated Boss
and Worker protobuf files match their sources. All RPC/integration tests manage
their own servers, available ports, and temporary storage. No separately started
Worker is needed locally or in CI. The Worker RPC tests are included in unittest
discovery and handle asynchronous execution states.
