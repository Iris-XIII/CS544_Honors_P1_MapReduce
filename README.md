# CS544 Honors MapReduce

A Python/gRPC MapReduce implementation in progress. See [design.md](design.md)
for the system design and planned speculative execution feature.

## Current scope

The Boss accepts jobs and reports their registration status. Jobs stay
`JOB_QUEUED`: scheduling, Worker execution integration, the client CLI, and Docker
Compose deployment are subsequent work. This is not yet an end-to-end MapReduce
runner. Job records live only in memory and are lost when the Boss restarts.

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

## Try submission and status lookup

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
  future write permissions. Use a distinct output directory for each real job.
- Submission does not execute user code or verify that its map/reduce functions
  are valid. Execution errors will be handled by the Worker/scheduler later.
- Invalid submissions and empty status-query IDs return gRPC `INVALID_ARGUMENT`.
  An unknown job ID returns `NOT_FOUND`.
- Each successful submission creates a new job, even if the settings are
  identical. Submission retries are not deduplicated.
- Job states are `JOB_QUEUED`, `JOB_RUNNING`, `JOB_COMPLETED`, and `JOB_FAILED`.
  Only `JOB_QUEUED` is currently produced. `JOB_UNKNOWN` is the protobuf default;
  missing jobs produce `NOT_FOUND` instead.

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
5. **Next PR: scheduling.** Add a task queue, assign tasks to idle Workers, poll
   attempts, and update job records under the store's lock. Start reduces only
   after all logical map tasks succeed. Count logical tasks rather than attempts
   so future speculative execution does not inflate progress.

## Tests

```bash
python -m unittest discover -s tests -p 'test_*.py' -v
```

The GitHub Actions workflow runs these tests on PRs and checks that generated Boss
protobuf files match their source. The existing Worker RPC test is a standalone
script, not a unittest case. Run it separately with a fresh Worker server:

```bash
# Terminal 1 (environment and PYTHONPATH set as above)
python -m worker.worker
# Terminal 2 (same setup)
python tests/test_worker_rpc.py
```

Restart the Worker before repeating its smoke test, because it uses fixed task
IDs and the Worker deliberately rejects duplicate attempts.
