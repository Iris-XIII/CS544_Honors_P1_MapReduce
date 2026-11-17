
import os
import sys
import time
import threading
from pathlib import Path
from concurrent import futures

import grpc

# Project root: contains worker/, proto/, and tests/
ROOT = Path(__file__).resolve().parents[1]

# Shared storage location (configurable for local testing).
# Defaults to /shared inside Docker containers.
SHARED_ROOT = Path(
    os.environ.get("SHARED_ROOT", "/shared")
)

# Allow imports when running from the project root.
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "proto"))

import mapreduce_pb2 as pb2
import mapreduce_pb2_grpc as pb2_grpc

from worker.mapper import execute_map
from worker.shuffle import partition_intermediate
from worker.reducer import execute_reduce


class WorkerService(pb2_grpc.WorkerServiceServicer):
    """
    MapReduce Worker gRPC service.

    Accepts map/reduce tasks asynchronously, tracks execution
    status, and writes attempt-specific outputs.
    """

    def __init__(self, max_tasks=1):
        # Key: (job_id, task_id, attempt_id, is_reduce)
        self.tasks = {}
        self.lock = threading.Lock()

        # Allow one active MapReduce task per worker.
        self.executor = futures.ThreadPoolExecutor(
            max_workers=max_tasks
        )

    def _submit_task(self, request, is_reduce):
        """Register and schedule a task attempt."""

        key = (
            request.job_id,
            request.task_id,
            request.attempt_id,
            is_reduce,
        )

        with self.lock:
            # Reject duplicate submissions of the same attempt.
            if key in self.tasks:
                return pb2.TaskReply(
                    accepted=False,
                    message="Task attempt already exists",
                )

            self.tasks[key] = {
                "state": pb2.PENDING,
                "created_at": time.monotonic(),
                "started_at": None,
                "finished_at": None,
                "output_path": "",
                "error": "",
            }

        # Execute the task asynchronously.
        self.executor.submit(
            self._execute_task,
            key,
            request,
            is_reduce,
        )

        task_type = "REDUCE" if is_reduce else "MAP"
        print(
            f"[Worker] Accepted {task_type} task "
            f"{request.task_id}, attempt {request.attempt_id}, "
            f"job {request.job_id}",
            flush=True,
        )

        # Acceptance does not mean execution has completed.
        return pb2.TaskReply(
            accepted=True,
            message=f"{task_type} task accepted",
        )

    def RunMap(self, request, context):
        """Accept and schedule a map task."""

        if request.num_reducers <= 0:
            return pb2.TaskReply(
                accepted=False,
                message="num_reducers must be positive",
            )

        if (
            request.start_line < 0
            or request.end_line < request.start_line
        ):
            return pb2.TaskReply(
                accepted=False,
                message="Invalid input line range",
            )

        if not request.input_path or not request.job_file:
            return pb2.TaskReply(
                accepted=False,
                message="input_path and job_file are required",
            )

        return self._submit_task(request, is_reduce=False)

    def RunReduce(self, request, context):
        """Accept and schedule a reduce task."""

        if not request.job_file or not request.output_path:
            return pb2.TaskReply(
                accepted=False,
                message="job_file and output_path are required",
            )

        # An empty list is allowed for reducers with no input.
        # The reducer will produce an empty output file.
        return self._submit_task(request, is_reduce=True)

    def _execute_task(self, key, request, is_reduce):
        """Execute a task and update its status."""

        started_at = time.monotonic()

        with self.lock:
            self.tasks[key]["state"] = pb2.RUNNING
            self.tasks[key]["started_at"] = started_at

        task_type = "REDUCE" if is_reduce else "MAP"

        print(
            f"[Worker] Running {task_type} task "
            f"{request.task_id}, attempt {request.attempt_id}",
            flush=True,
        )

        try:
            if is_reduce:
                output_path = self._execute_reduce(request)
            else:
                output_path = self._execute_map(request)

            finished_at = time.monotonic()

            with self.lock:
                self.tasks[key]["output_path"] = str(output_path)
                self.tasks[key]["finished_at"] = finished_at
                self.tasks[key]["state"] = pb2.COMPLETED

            print(
                f"[Worker] Completed {task_type} task "
                f"{request.task_id}, attempt {request.attempt_id} "
                f"in {finished_at - started_at:.3f}s",
                flush=True,
            )

        except Exception as exc:
            finished_at = time.monotonic()

            with self.lock:
                self.tasks[key]["finished_at"] = finished_at
                self.tasks[key]["state"] = pb2.FAILED
                self.tasks[key]["error"] = str(exc)

            print(
                f"[Worker] Failed {task_type} task "
                f"{request.task_id}, attempt {request.attempt_id}: "
                f"{exc}",
                flush=True,
            )


    def _execute_map(self, request):
        """Execute map() and partition intermediate output."""

        job_dir = (
            SHARED_ROOT / "intermediate" / request.job_id
        )
        job_dir.mkdir(parents=True, exist_ok=True)

        # Separate temporary output for each map attempt.
        raw_path = job_dir / (
            f"map_{request.task_id}_"
            f"attempt_{request.attempt_id}_raw.jsonl"
        )



        # Optional artificial delay for straggler simulation.
        # Disabled by default, so normal Worker behavior is unchanged.
        delay_seconds = float(os.environ.get("MAP_DELAY_SECONDS", "0"))
        delay_task_id = int(os.environ.get("MAP_DELAY_TASK_ID", "0"))
        delay_attempt_id = int(os.environ.get("MAP_DELAY_ATTEMPT_ID", "0"))

        if (
            delay_seconds > 0
            and request.task_id == delay_task_id
            and request.attempt_id == delay_attempt_id
        ):
            print(
                f"[Worker] Simulating straggler: "
                f"MAP task {request.task_id}, "
                f"attempt {request.attempt_id}, "
                f"sleeping {delay_seconds}s",
                flush=True,
            )
            time.sleep(delay_seconds)




        execute_map(
            input_path=request.input_path,
            job_file=request.job_file,
            start_line=request.start_line,
            end_line=request.end_line,
            output_path=raw_path,
        )

        # Create reducer-specific intermediate partitions.
        partition_dir = job_dir / (
            f"map_{request.task_id}_"
            f"attempt_{request.attempt_id}"
        )

        partition_intermediate(
            input_path=raw_path,
            output_dir=partition_dir,
            task_id=request.task_id,
            attempt_id=request.attempt_id,
            num_reducers=request.num_reducers,
        )

        return partition_dir

    def _execute_reduce(self, request):
        """Execute reduce() using selected mapper output files."""

        output_dir = Path(request.output_path)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Separate output for each speculative reduce attempt.
        output_path = output_dir / (
            f"reduce_{request.task_id}_"
            f"attempt_{request.attempt_id}.jsonl"
        )

        execute_reduce(
            input_paths=list(request.input_paths),
            job_file=request.job_file,
            output_path=output_path,
        )

        return output_path

    def GetTaskStatus(self, request, context):
        """Return execution state and elapsed time."""

        key = (
            request.job_id,
            request.task_id,
            request.attempt_id,
            request.is_reduce,
        )

        with self.lock:
            task = self.tasks.get(key)

            if task is None:
                return pb2.TaskStatusReply(
                    state=pb2.UNKNOWN,
                    error="Task attempt not found",
                )

            start = task["started_at"]
            end = task["finished_at"]

            # Elapsed execution time stops increasing
            # once the task completes or fails.
            elapsed = (
                (
                    end if end is not None
                    else time.monotonic()
                ) - start
                if start is not None
                else 0.0
            )

            return pb2.TaskStatusReply(
                state=task["state"],
                elapsed_seconds=elapsed,
                output_path=task["output_path"],
                error=task["error"],
            )


def serve():
    """Start the Worker gRPC server."""

    port = int(os.environ.get("WORKER_PORT", "50051"))

    # Threads for handling incoming gRPC requests.
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=4)
    )

    # Separate executor: one active MapReduce task.
    pb2_grpc.add_WorkerServiceServicer_to_server(
        WorkerService(max_tasks=1),
        server,
    )

    bound_port = server.add_insecure_port(
        f"[::]:{port}"
    )

    if bound_port == 0:
        raise RuntimeError(
            f"Unable to bind Worker port {port}"
        )

    server.start()

    print(
        f"[Worker] Listening on port {port}",
        flush=True,
    )

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("[Worker] Shutting down...", flush=True)
        server.stop(grace=2)


if __name__ == "__main__":
    serve()
