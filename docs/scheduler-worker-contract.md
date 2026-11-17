# Baseline Boss/Worker integration contract

The scheduler follows Iris's PR #5 shuffle/reducer code and PR #6 executing
Worker interface. It uses PR #6's `ReduceTaskRequest.input_paths` field instead
of the earlier manifest proposal. The executing Worker and its self-contained
tests reached `main` through PR #8 and are included in this branch.

## Assignment and status

- Boss pushes `RunMap` or `RunReduce` to an idle Worker, then polls `GetTaskStatus`.
- Identity is `(job_id, task_id, attempt_id, is_reduce)`. Map and reduce IDs each
  start at zero. Baseline attempts always use `attempt_id=0`.
- The Boss assigns at most one task per Worker. `accepted=True` means registered,
  not completed. `PENDING`/`RUNNING` mean keep waiting; `FAILED` includes an error.
- Report `COMPLETED` only after output files are closed and visible. A map returns
  its partition directory; a reducer returns its actual output filename.
- All paths are absolute and refer to the same shared files from Boss and Workers.
  Keep input and user-code files unchanged and available until the job finishes.

## Maps and shuffle

Boss counts physical UTF-8 lines and assigns half-open `[start_line, end_line)`
ranges. For seven lines and three maps: `[0,3)`, `[3,5)`, `[5,7)`. Empty ranges
are valid. Workers execute the user `map()` function and partition the emitted
records using PR #5's stable SHA-256 key hash.

`MapTaskRequest` has no output-location field. PR #6's Worker writes beneath
`SHARED_ROOT` (default `/shared`) and returns this directory:

```text
<SHARED_ROOT>/intermediate/<job_id>/map_<map_id>_attempt_<attempt_id>/
```

Inside it, PR #5's shuffle creates one file per reducer, even when empty:

```text
map_<map_id>_attempt_<attempt_id>_reduce_<reducer_id>.jsonl
```

The Boss uses the returned directory and these filenames. It does not rehash
keys or read the intermediate records. Missing partition files fail the job
before any reducer is started.

## Reduce requests

After every map succeeds, each reducer gets its partition from each selected
map attempt. For reducer 1 of a two-map job:

```python
ReduceTaskRequest(
    job_id="example-job",
    task_id=1,
    attempt_id=0,
    job_file="/shared/jobs/wordcount.py",
    input_paths=[
        "/shared/intermediate/example-job/map_0_attempt_0/map_0_attempt_0_reduce_1.jsonl",
        "/shared/intermediate/example-job/map_1_attempt_0/map_1_attempt_0_reduce_1.jsonl",
    ],
    output_path="/shared/intermediate/example-job/reduce",
)
```

- `input_paths` is the complete, explicitly selected file list for that reducer.
  Never scan all attempt directories, because that could include duplicate data.
- `intermediate_path` is unused and left empty. No manifest parsing is required.
- `output_path` is a **directory**, matching PR #6. The Worker appends
  `reduce_<task_id>_attempt_<attempt_id>.jsonl` and returns that full filename.
- A reducer does not need the total reducer count: its inputs are already selected.
- The user reducer returns `(key, value)` or `None`; it does not yield pairs.

Future speculation can select one successful attempt per logical map and use
its partition files. This baseline always selects attempt zero and does not
launch speculative tasks or retries.

## Final results

The scheduler's workspace is `<work_dir>/<job_id>/`. `work_dir` defaults to
`/shared/intermediate`, and may share that root with the Worker or be a separate
shared location. The user-requested final output directory must not overlap the
job workspace and must start empty.

Boss passes `<work_dir>/<job_id>/reduce` as the reduce output directory, validates
each returned filename against its task/attempt identity, and waits for every
reducer to succeed. Then it publishes `part-00000.jsonl`, `part-00001.jsonl`, etc.
in the user's output directory and marks the job `JOB_COMPLETED`.

Publication uses a temporary file and atomic rename for each part; it is not a
transaction across all parts. A filesystem failure can leave partial results.
Clients must wait for `JOB_COMPLETED`, not just full task counters, before using
the output. Worker failures before publication leave final results unpublished.

## Failures and tests

The first observed rejection, failed/unknown attempt, missing output, RPC error,
or timeout fails the job. There are no retries or cancellation RPCs. Workers with
unresolved attempts are disabled for that Boss process; explicitly terminal
Workers remain usable for later jobs. Workspaces are kept for debugging.

`tests/test_scheduler.py` checks scheduling and failure cases with controlled
gRPC Workers. `tests/test_boss_worker_integration.py` checks the real pipeline
through Boss RPCs when PR #6's executing Worker is available. It creates and
cleans up its own servers, available ports, executors, and temporary storage.
The tests skip explicitly on the old Worker skeleton. Run Workers manually as
`python -m worker.worker` from the repository root, as requested in the #4 review.
