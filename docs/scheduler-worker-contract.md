# Baseline Boss/Worker integration contract

This is the handoff **proposed by the Boss scheduler implementation**. It uses
the existing Worker protobuf fields without changing Iris's `.proto`. Iris's
Worker skeleton does not yet implement execution or this handoff. Confirm this
contract together before connecting the production map/shuffle/reduce code.

## Assignment and status

- Boss pushes `RunMap` or `RunReduce` to an idle Worker, then polls `GetTaskStatus`.
- Task identity is `(job_id, task_id, attempt_id, is_reduce)`. Map and reduce task
  IDs each start at zero. For baseline scheduling, `attempt_id` is always zero.
- An accepted request reserves one Worker slot until it reports `COMPLETED` or
  `FAILED`. Acceptance is not completion. There should be one active task per
  Worker, and the handlers must return promptly so status polling can continue.
- `PENDING`/`RUNNING` mean keep waiting. `FAILED` includes an error message.
  `UNKNOWN`, rejected requests, RPC errors, and elapsed task timeouts fail the job.
- Report `COMPLETED` only after output files are closed and visible in shared
  storage. A completed map must return an absolute `output_path` referring to its
  regular file or partition directory. A completed reducer must return its
  assigned regular file.
- Paths must identify the same files from the Boss and all Workers. Input and
  user-code files must remain available and unchanged for the duration of a job.

## Maps

Boss counts physical UTF-8 text lines and assigns zero-based, half-open ranges
`[start_line, end_line)`. For seven lines and three maps, ranges are `[0,3)`,
`[3,5)`, `[5,7)`. Empty ranges are valid and must still complete with an empty
output file or directory.

Worker execution owns reading those ranges, running user `map()`, and shuffle
partitioning. The Boss does not import user code or read intermediate records.

`MapTaskRequest` has no output-location field, so the Worker chooses the map
output location. Include the job, logical map, and attempt IDs to isolate writes.
For example:

```text
/shared/intermediate/<job_id>/map-00002/attempt-0/
```

The Boss uses the returned `output_path`; it does not assume that naming layout.
The map-output encoding (single JSONL file vs. per-reducer partition directory)
and deterministic partition function are Worker-side decisions shared by mapper
and reducer implementations. The manifest works with either layout, but the
two Worker components must agree on one.

## Map-to-reduce handoff

After **every map succeeds**, the Boss writes:

```text
<work_dir>/<job_id>/map-outputs.json
```

Example contents:

```json
{
  "version": 1,
  "job_id": "example-job",
  "num_reducers": 2,
  "map_outputs": [
    {
      "task_id": 0,
      "attempt_id": 0,
      "output_path": "/shared/intermediate/example-job/map-00000/attempt-0/"
    },
    {
      "task_id": 1,
      "attempt_id": 0,
      "output_path": "/shared/intermediate/example-job/map-00001/attempt-0/"
    }
  ]
}
```

For a reduce assignment:

- `intermediate_path` is the **manifest file path**, not a directory to scan.
- `task_id` is the zero-based reducer partition index.
- `num_reducers` comes from the manifest (the current reduce request has no count).
- The reducer reads only the listed map outputs, taking its partition from each
  map. It must not glob every attempt directory under shared storage.

The current scheduler lists attempt zero once per logical map. A future
speculative scheduler can choose a winning attempt and list only that output.
Winner selection and speculative execution are not implemented in this PR.

## Reduce outputs and publication

Boss assigns each reducer an attempt-specific filename:

```text
<work_dir>/<job_id>/reduce/part-00000-attempt-0.jsonl
```

Worker writes exactly that file (even for empty output) and returns its path.
After every reducer succeeds, the Boss publishes files in the user-requested
output directory as `part-00000.jsonl`, `part-00001.jsonl`, etc., and marks the job
`JOB_COMPLETED`. Reduce completion counts can reach their total before file
publication finishes; clients must wait for the job state.

Output directories must start empty. There is no rollback across all output
files if filesystem publication fails; clients must treat `JOB_FAILED` output as
incomplete. Worker execution failures happen before publication and do not
publish partial reducer results.

## Baseline failure handling

The Boss fails a job on the first observed error. No retries, cancellation, or
Worker recovery are implemented. Other attempts may still be running, so the
Boss disables their Workers rather than declaring them idle. Explicitly terminal
Workers remain usable for subsequent jobs. Workspaces are kept for debugging.

Tests in `tests/test_scheduler.py` exercise this contract using controlled Worker
servers over gRPC. A real end-to-end word-count test is still needed once the
Worker execution PR implements the agreed contract.
