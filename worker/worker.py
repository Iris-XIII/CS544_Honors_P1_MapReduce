
import time
import threading
from concurrent import futures

import grpc

import mapreduce_pb2
import mapreduce_pb2_grpc


class WorkerService(mapreduce_pb2_grpc.WorkerServiceServicer):
    """
    Minimal MapReduce Worker gRPC service.

    Supports task submission and status tracking.
    Actual map/reduce execution will be added later.
    """

    def __init__(self):
        # Key: (job_id, task_id, attempt_id, is_reduce)
        # Value: task state and timing information
        self.tasks = {}
        self.lock = threading.Lock()

    def _submit_task(self, request, is_reduce):
        """Register a task attempt without executing it yet."""

        key = (
            request.job_id,
            request.task_id,
            request.attempt_id,
            is_reduce,
        )

        with self.lock:
            # Prevent duplicate submissions of the same attempt.
            if key in self.tasks:
                return mapreduce_pb2.TaskReply(
                    accepted=False,
                    message="Task attempt already exists",
                )

            self.tasks[key] = {
                "state": mapreduce_pb2.PENDING,
                "created_at": time.monotonic(),
                "output_path": "",
                "error": "",
            }

        task_type = "REDUCE" if is_reduce else "MAP"

        print(
            f"[Worker] Accepted {task_type} task "
            f"{request.task_id}, attempt {request.attempt_id}, "
            f"job {request.job_id}",
            flush=True,
        )

        return mapreduce_pb2.TaskReply(
            accepted=True,
            message=f"{task_type} task accepted",
        )

    def RunMap(self, request, context):
        """Accept a map task assignment from the boss."""
        return self._submit_task(request, is_reduce=False)

    def RunReduce(self, request, context):
        """Accept a reduce task assignment from the boss."""
        return self._submit_task(request, is_reduce=True)

    def GetTaskStatus(self, request, context):
        """Return the current state of a specific task attempt."""

        key = (
            request.job_id,
            request.task_id,
            request.attempt_id,
            request.is_reduce,
        )

        with self.lock:
            task = self.tasks.get(key)

            if task is None:
                return mapreduce_pb2.TaskStatusReply(
                    state=mapreduce_pb2.UNKNOWN,
                    error="Task attempt not found",
                )

            return mapreduce_pb2.TaskStatusReply(
                state=task["state"],
                elapsed_seconds=(
                    time.monotonic() - task["created_at"]
                ),
                output_path=task["output_path"],
                error=task["error"],
            )


def serve():
    """Start the gRPC Worker server."""

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10)
    )

    mapreduce_pb2_grpc.add_WorkerServiceServicer_to_server(
        WorkerService(),
        server,
    )

    port = server.add_insecure_port("[::]:50051")
    if port == 0:
        raise RuntimeError("Failed to bind Worker gRPC port 50051")

    server.start()

    print(
        "[Worker] gRPC server listening on port 50051",
        flush=True,
    )

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("\n[Worker] Shutting down...")
        server.stop(grace=2)


if __name__ == "__main__":
    serve()
