"""Thread-safe, in-memory job records, separate from the gRPC transport."""

from dataclasses import dataclass, field
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

    The scheduler will add state/progress updates in the next implementation.
    For now, registered jobs stay QUEUED and are never sent to Workers.
    """

    def __init__(self):
        self._jobs: dict[str, JobRecord] = {}
        self._lock = Lock()

    def create(self, spec: JobSpec) -> JobRecord:
        with self._lock:
            job_id = uuid4().hex
            while job_id in self._jobs:
                job_id = uuid4().hex
            record = JobRecord(job_id=job_id, spec=spec)
            self._jobs[job_id] = record
            return record

    def get(self, job_id: str) -> JobRecord:
        """Return a snapshot, or raise KeyError for an unknown ID."""
        with self._lock:
            return self._jobs[job_id]
