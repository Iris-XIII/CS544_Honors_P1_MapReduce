"""Single-job, push-based scheduling against the existing Worker gRPC API."""

from collections import deque
import logging
import math
from statistics import median
from pathlib import Path
import shutil
from threading import Event, Thread
from time import monotonic

import grpc

import mapreduce_pb2 as worker_pb2
import mapreduce_pb2_grpc as worker_grpc
from boss.jobs import JobStore


class JobFailed(Exception):
    """An execution failure that should be visible through GetJobStatus."""


def line_ranges(num_lines: int, num_maps: int) -> list[tuple[int, int]]:
    """Balanced, disjoint [start, end) ranges, including empty map tasks."""
    if num_lines < 0 or num_maps <= 0:
        raise ValueError("Line count must be nonnegative and map count positive")
    size, extra = divmod(num_lines, num_maps)
    ranges = []
    start = 0
    for task_id in range(num_maps):
        end = start + size + (task_id < extra)
        ranges.append((start, end))
        start = end
    return ranges


class Scheduler:
    """One background thread owns task assignment; RPC threads own submission.

    Optional map-only speculation runs one duplicate of an unusually slow map.
    Unfinished losing attempts are tracked until terminal, so their Workers
    cannot accidentally receive overlapping tasks (there is no cancellation RPC).
    """

    def __init__(self, store: JobStore, worker_addresses, work_dir,
                 poll_interval=0.2, rpc_timeout=3.0, task_timeout=300.0,
                 speculate=False, straggler_seconds=1.0):
        addresses = list(worker_addresses)
        if not addresses or any(not address.strip() for address in addresses):
            raise ValueError("At least one nonempty Worker address is required")
        if len(set(addresses)) != len(addresses):
            raise ValueError("Worker addresses must be unique")
        for name, value in (("poll_interval", poll_interval),
                            ("rpc_timeout", rpc_timeout),
                            ("task_timeout", task_timeout),
                            ("straggler_seconds", straggler_seconds)):
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and positive")
        self.work_dir = Path(work_dir)
        if not self.work_dir.is_absolute():
            raise ValueError("work_dir must be an absolute shared-storage path")
        self.store = store
        self.poll_interval = poll_interval
        self.rpc_timeout = rpc_timeout
        self.task_timeout = task_timeout
        self.speculate = speculate
        self.straggler_seconds = straggler_seconds
        # Address -> (losing request, dispatch time, is_reduce). A phase can
        # finish while one of its speculative attempts is still running.
        self._lingering = {}
        self._channels = [grpc.insecure_channel(address) for address in addresses]
        self._workers = dict(zip(addresses, (
            worker_grpc.WorkerServiceStub(channel) for channel in self._channels
        )))
        self._disabled = set()
        self._stop = Event()
        self._thread = Thread(target=self._run, name="boss-scheduler", daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        """Stop dispatching, fail the active job, and close Worker connections."""
        self._stop.set()
        if self._thread.ident is not None:
            self._thread.join()
        for channel in self._channels:
            channel.close()

    def _check_stopping(self):
        if self._stop.is_set():
            raise JobFailed("Boss scheduler stopped")

    def _run(self):
        while not self._stop.is_set():
            record = self.store.claim_next()
            if record is None:
                self._stop.wait(self.poll_interval)
                continue
            try:
                self._run_job(record)
            except Exception as exc:
                # Keep the scheduler alive for later queued jobs, including after
                # filesystem errors. Expose a useful failure through the Boss API.
                self.store.fail(record.job_id, str(exc) or type(exc).__name__)
                logging.warning("Job %s failed: %s", record.job_id, exc)

    def _run_job(self, record):
        self._check_stopping()
        if all(address in self._disabled for address in self._workers):
            raise JobFailed("No usable Workers remain; restart affected Workers and the Boss")
        spec = record.spec
        output_dir = Path(spec.output_path)
        workspace = self.work_dir / record.job_id
        # Validate the layout before creating directories for the job.
        if (workspace.resolve().is_relative_to(output_dir.resolve())
                or output_dir.resolve().is_relative_to(workspace.resolve())):
            raise JobFailed("Job workspace and output directory must not overlap")
        output_dir.mkdir(parents=True, exist_ok=True)
        if any(output_dir.iterdir()):
            raise JobFailed("Output directory must be empty; refusing to overwrite existing data")
        workspace.mkdir(parents=True, exist_ok=False)

        num_lines = 0
        with open(spec.input_path, encoding="utf-8") as source:
            for num_lines, _ in enumerate(source, start=1):
                if num_lines % 10000 == 0:
                    self._check_stopping()
        maps = [worker_pb2.MapTaskRequest(
            job_id=record.job_id, task_id=task_id, attempt_id=0,
            input_path=spec.input_path, job_file=spec.job_file,
            num_reducers=spec.num_reducers, start_line=start, end_line=end,
        ) for task_id, (start, end) in enumerate(line_ranges(num_lines, spec.num_maps))]
        map_outputs = self._run_phase(record.job_id, maps, is_reduce=False)

        attempts = workspace / "reduce"
        attempts.mkdir()
        reduces = []
        for reducer_id in range(spec.num_reducers):
            # Match Iris's shuffle filenames. Select one successful attempt per
            # logical map; never glob other attempts into the reducer's input.
            input_paths = []
            for map_id, (directory, attempt_id) in sorted(map_outputs.items()):
                partition = Path(directory) / (
                    f"map_{map_id}_attempt_{attempt_id}_reduce_{reducer_id}.jsonl"
                )
                if not partition.is_file():
                    raise JobFailed(f"Map {map_id} is missing reducer {reducer_id} partition: {partition}")
                input_paths.append(str(partition))
            reduces.append(worker_pb2.ReduceTaskRequest(
                job_id=record.job_id, task_id=reducer_id, attempt_id=0,
                input_paths=input_paths,
                output_path=str(attempts),  # Worker creates its attempt file here.
                job_file=spec.job_file,
            ))
        reduce_outputs = self._run_phase(record.job_id, reduces, is_reduce=True)

        # Publish only after every reducer succeeds. Copy into a temporary file
        # beside each destination so final rename is atomic even across mounts.
        for task_id, source in sorted(reduce_outputs.items()):
            self._check_stopping()
            destination = output_dir / f"part-{task_id:05d}.jsonl"
            temporary = output_dir / f".{record.job_id}-{task_id}.tmp"
            shutil.copyfile(source, temporary)
            temporary.replace(destination)
        self._check_stopping()
        self.store.complete(record.job_id)

    def _reap_lingering(self):
        """Reclaim Workers after their losing map attempts finish, without waiting.

        An unreachable/expired Worker is disabled, rather than being reused
        while it might still be executing an old attempt.
        """
        for address, (request, started_at, is_reduce) in list(self._lingering.items()):
            remaining = self.task_timeout - (monotonic() - started_at)
            if remaining <= 0:
                self._disabled.add(address)
                del self._lingering[address]
                continue
            try:
                status = self._workers[address].GetTaskStatus(
                    worker_pb2.TaskStatusRequest(
                        job_id=request.job_id, task_id=request.task_id,
                        attempt_id=request.attempt_id, is_reduce=is_reduce,
                    ), timeout=min(self.rpc_timeout, remaining),
                )
            except grpc.RpcError:
                self._disabled.add(address)
                del self._lingering[address]
                continue
            if status.state in (worker_pb2.COMPLETED, worker_pb2.FAILED):
                del self._lingering[address]
            elif status.state not in (worker_pb2.PENDING, worker_pb2.RUNNING):
                self._disabled.add(address)
                del self._lingering[address]

    def _run_phase(self, job_id, requests, is_reduce):
        pending = deque(requests)
        total = len(requests)
        # Address -> (attempt request, dispatch time); a Worker runs one attempt.
        active = {}
        # Map values include the winning attempt id for correct shuffle input.
        # Reduce values remain final result filenames.
        outputs = {}
        completed_durations = []
        speculated = set()
        phase = "reduce" if is_reduce else "map"

        def dispatch(address, request):
            active[address] = (request, monotonic())
            rpc = self._workers[address].RunReduce if is_reduce else self._workers[address].RunMap
            reply = rpc(request, timeout=min(self.rpc_timeout, self.task_timeout))
            if not reply.accepted:
                raise JobFailed(
                    f"Worker {address} rejected {phase} {request.task_id} "
                    f"attempt {request.attempt_id}: {reply.message}"
                )

        try:
            # The phase is logically complete as soon as every task has one
            # successful attempt. We must not wait for losing duplicates.
            while len(outputs) < total:
                self._check_stopping()
                self._reap_lingering()

                # Always give original, pending tasks priority over speculation.
                for address in self._workers:
                    if not pending:
                        break
                    if address in self._disabled or address in self._lingering or address in active:
                        continue
                    dispatch(address, pending.popleft())

                for address, (request, started_at) in list(active.items()):
                    self._check_stopping()
                    remaining = self.task_timeout - (monotonic() - started_at)
                    if remaining <= 0:
                        raise JobFailed(
                            f"Worker {address} {phase} {request.task_id} "
                            f"attempt {request.attempt_id} timed out"
                        )
                    status = self._workers[address].GetTaskStatus(
                        worker_pb2.TaskStatusRequest(
                            job_id=job_id, task_id=request.task_id,
                            attempt_id=request.attempt_id, is_reduce=is_reduce,
                        ), timeout=min(self.rpc_timeout, remaining),
                    )
                    if status.state == worker_pb2.FAILED:
                        del active[address]
                        # A failed attempt is harmless if the task already has
                        # a winner or its other attempt is still running.
                        another_running = any(
                            other.task_id == request.task_id
                            for other, _ in active.values()
                        )
                        if request.task_id not in outputs and not another_running:
                            raise JobFailed(
                                f"Worker {address} {phase} {request.task_id} "
                                f"attempt {request.attempt_id} failed: {status.error}"
                            )
                        continue
                    if status.state == worker_pb2.COMPLETED:
                        del active[address]
                        if request.task_id in outputs:
                            # The other attempt already won; ignore this output.
                            continue
                        path = Path(status.output_path)
                        if (not status.output_path or not path.is_absolute()
                                or not (path.is_file() or path.is_dir())):
                            raise JobFailed(f"Worker {address} returned a missing/invalid output path")
                        if is_reduce:
                            expected = Path(request.output_path) / (
                                f"reduce_{request.task_id}_attempt_{request.attempt_id}.jsonl"
                            )
                            if path != expected or not path.is_file():
                                raise JobFailed(f"Worker {address} did not write the assigned reduce output")
                            outputs[request.task_id] = str(path)
                        else:
                            if not path.is_dir():
                                raise JobFailed(f"Worker {address} map output must be a partition directory")
                            outputs[request.task_id] = (str(path), request.attempt_id)
                        completed_durations.append(monotonic() - started_at)
                        self.store.task_completed(job_id, request.task_id, is_reduce)
                        logging.info(
                            "Accepted %s task %s attempt %s from %s",
                            phase, request.task_id, request.attempt_id, address,
                        )
                    elif status.state not in (worker_pb2.PENDING, worker_pb2.RUNNING):
                        raise JobFailed(
                            f"Worker {address} lost {phase} {request.task_id} or returned an unknown state"
                        )

                # Speculate only maps, only when no original work is waiting,
                # and only once per logical task on a *different* idle Worker.
                # Compare elapsed time with completed peer maps (median); the
                # absolute minimum avoids duplicating naturally short tasks.
                if self.speculate and not is_reduce and not pending and completed_durations:
                    cutoff = max(self.straggler_seconds, 1.5 * median(completed_durations))
                    for address in self._workers:
                        if (address in self._disabled or address in self._lingering
                                or address in active):
                            continue
                        candidates = [
                            (monotonic() - started, original)
                            for original, started in active.values()
                            if original.task_id not in outputs
                            and original.task_id not in speculated
                            and original.attempt_id == 0
                            and monotonic() - started >= cutoff
                        ]
                        if not candidates:
                            break
                        _, original = max(candidates, key=lambda item: item[0])
                        duplicate = worker_pb2.MapTaskRequest()
                        duplicate.CopyFrom(original)
                        duplicate.attempt_id = 1
                        logging.info(
                            "Straggler detected: map task %s; launching attempt 1 on %s",
                            original.task_id, address,
                        )
                        dispatch(address, duplicate)
                        speculated.add(original.task_id)

                if len(outputs) < total:
                    if not active and not pending:
                        raise JobFailed(f"No running {phase} attempts remain")
                    # When all Workers are occupied by older losing attempts,
                    # keep waiting for them to finish without double assigning.
                    if not active and pending:
                        available = [a for a in self._workers if a not in self._disabled]
                        if not available:
                            raise JobFailed("No usable Workers remain")
                    self._stop.wait(self.poll_interval)
        except grpc.RpcError as exc:
            raise JobFailed(f"Worker {address} RPC failed: {exc.code().name}: {exc.details()}") from exc
        finally:
            # No cancellation RPC: never schedule other work on a Worker that
            # might still be running its unfinished attempt.
            if len(outputs) == total:
                self._lingering.update(
                    (address, (request, started, is_reduce))
                    for address, (request, started) in active.items()
                )
            else:
                self._disabled.update(active)
        return outputs
