"""Thread-safe, in-memory job records, separate from the gRPC transport."""

from collections import deque
from dataclasses import dataclass, field, replace
from threading import Lock
from time import time
from uuid import uuid4

import boss_pb2


@dataclass(frozen=True)
class JobSpec:
    """The five settings supplied by the client, after validation."""

    input_path: str
    output_path: str
    job_file: str
    num_maps: int
    num_reducers: int


@dataclass(frozen=True)
class JobRecord:
    """An immutable snapshot: callers cannot change the store without its lock."""

    job_id: str
    spec: JobSpec
    state: int = boss_pb2.JOB_QUEUED
    completed_maps: int = 0
    completed_reducers: int = 0
    error: str = ""
    submitted_at_unix_seconds: float = field(default_factory=time)


class JobStore:
    """Records live for this Boss process only; restarting it loses all jobs.

    Registration and queue insertion happen together under the lock. The single
    scheduler claims jobs in submission order and replaces immutable snapshots.
    """

    def __init__(self):
        self._jobs: dict[str, JobRecord] = {}
        self._pending: deque[str] = deque()
        self._completed_tasks: dict[str, set[tuple[bool, int]]] = {}
        self._lock = Lock()

    def create(self, spec: JobSpec) -> JobRecord:
        with self._lock:
            job_id = uuid4().hex
            while job_id in self._jobs:
                job_id = uuid4().hex
            record = JobRecord(job_id=job_id, spec=spec)
            self._jobs[job_id] = record
            self._pending.append(job_id)
            self._completed_tasks[job_id] = set()
            return record

    def get(self, job_id: str) -> JobRecord:
        """Return a snapshot, or raise KeyError for an unknown ID."""
        with self._lock:
            return self._jobs[job_id]

    def claim_next(self) -> JobRecord | None:
        """Atomically take the oldest queued job and mark it RUNNING."""
        with self._lock:
            while self._pending:
                job_id = self._pending.popleft()
                record = self._jobs[job_id]
                if record.state == boss_pb2.JOB_QUEUED:
                    record = replace(record, state=boss_pb2.JOB_RUNNING)
                    self._jobs[job_id] = record
                    return record
            return None

    def task_completed(self, job_id: str, task_id: int, is_reduce: bool):
        """Count each logical task once, even if completion is reported twice."""
        with self._lock:
            record = self._jobs[job_id]
            if record.state != boss_pb2.JOB_RUNNING:
                raise ValueError("Task completion requires a running job")
            total = record.spec.num_reducers if is_reduce else record.spec.num_maps
            if not 0 <= task_id < total:
                raise ValueError("Task ID is outside the job's task range")
            if is_reduce and record.completed_maps != record.spec.num_maps:
                raise ValueError("All maps must complete before reducers")
            key = (is_reduce, task_id)
            if key in self._completed_tasks[job_id]:
                return
            self._completed_tasks[job_id].add(key)
            field_name = "completed_reducers" if is_reduce else "completed_maps"
            self._jobs[job_id] = replace(
                record, **{field_name: getattr(record, field_name) + 1}
            )

    def complete(self, job_id: str):
        """Finish only after all task outputs have been successfully published."""
        with self._lock:
            record = self._jobs[job_id]
            if (record.state != boss_pb2.JOB_RUNNING
                    or record.completed_maps != record.spec.num_maps
                    or record.completed_reducers != record.spec.num_reducers):
                raise ValueError("Cannot complete a job with unfinished tasks")
            self._jobs[job_id] = replace(record, state=boss_pb2.JOB_COMPLETED)

    def fail(self, job_id: str, error: str):
        with self._lock:
            record = self._jobs[job_id]
            if record.state not in (boss_pb2.JOB_COMPLETED, boss_pb2.JOB_FAILED):
                self._jobs[job_id] = replace(
                    record, state=boss_pb2.JOB_FAILED, error=error
                )
