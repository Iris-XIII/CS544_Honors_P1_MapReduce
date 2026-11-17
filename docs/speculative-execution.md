# Boss-side speculative map execution

This feature is **off by default** so the baseline can be measured without duplicate work.
No changes to the Worker gRPC protocol are required.

## Detection and scheduling

- The Boss records each map attempt's dispatch time.
- After at least one peer map has completed, a running map is treated as a
  straggler when its elapsed time exceeds **both** the configured minimum
  (`BOSS_STRAGGLER_SECONDS`, default 1 second) and 1.5 times the median duration
  of successfully completed maps in that job.
- Original map tasks have priority. An eligible duplicate is sent to an **idle
  Worker** as `attempt_id=1`; each logical map may receive at most one duplicate.
- The first **successful** attempt is the winner. The JobStore counts the logical
  map only once. Reducers get only that attempt's explicitly selected partitions.
- If one attempt fails while its sibling is still running, the sibling may still
  win. If no successful/running attempts remain, the job fails.
- A losing attempt is not cancelled: the Boss temporarily reserves that Worker
  until its old attempt reaches a terminal state. If it becomes unreachable or
  exceeds the configured timeout, the Boss disables it to prevent overlap.
- Only **map** tasks are speculated. Reduce tasks are unchanged.

## Enable it for Iris's straggler simulation

The existing `compose.straggler.yaml` delays **Worker 1's map task 0 attempt 0**
by eight seconds. The duplicate (`attempt_id=1`) will not receive that delay.

```bash
# Baseline: original map task 0 takes ~8 seconds.
LOCAL_UID=$(id -u) LOCAL_GID=$(id -g) BOSS_SPECULATE=0 \
  docker compose -f compose.yaml -f compose.straggler.yaml up --build -d

# After running and recording the baseline, enable speculative scheduling.
LOCAL_UID=$(id -u) LOCAL_GID=$(id -g) BOSS_SPECULATE=1 BOSS_STRAGGLER_SECONDS=1 \
  docker compose -f compose.yaml -f compose.straggler.yaml up -d --force-recreate boss
```

Use a new empty output directory for each client job. Submit 4 map tasks and
2 reduce tasks with the same WordCount input and compare elapsed job runtimes;
use the **same straggler simulation** for both configurations.

View scheduling decisions with `docker compose logs boss` and the map attempt
logs with `docker compose logs worker1 worker2 worker3 worker4` (include
`-f compose.straggler.yaml` after `-f compose.yaml` when using the override).
Look for Boss log entries `Straggler detected` and `Accepted map task ... attempt 1`.

You can also launch Boss directly with `--speculate --straggler-seconds 1`.

## Tests

```bash
PYTHONPATH=proto:. python -m unittest discover -s tests -p 'test_scheduler.py' -v
PYTHONPATH=proto:. python -m unittest discover -s tests -p 'test_*.py' -v
```

`tests/test_scheduler.py` covers baseline/no duplicate, straggler detection and
speculative dispatch, choosing only the winning map partitions, failure of a
duplicate with fallback to the original, and safe reclamation of a losing
Worker. Docker end-to-end benchmarking remains a separate integration task.
