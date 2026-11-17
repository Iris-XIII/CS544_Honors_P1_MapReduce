
from concurrent import futures
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import grpc
import mapreduce_pb2_grpc as pb2_grpc

import worker.worker as worker_module


@contextmanager
def running_worker(shared_root):
    """Start a temporary Worker gRPC server for testing."""

    with patch.object(
        worker_module, "SHARED_ROOT", Path(shared_root)
    ):
        service = worker_module.WorkerService(max_tasks=1)

        server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=4)
        )

        pb2_grpc.add_WorkerServiceServicer_to_server(
            service, server
        )

        # Let the OS select a free port.
        port = server.add_insecure_port("127.0.0.1:0")

        if port == 0:
            raise RuntimeError("Could not bind test Worker")

        server.start()

        channel = grpc.insecure_channel(
            f"127.0.0.1:{port}"
        )

        try:
            grpc.channel_ready_future(channel).result(
                timeout=5
            )

            stub = pb2_grpc.WorkerServiceStub(channel)
            yield stub

        finally:
            channel.close()
            server.stop(grace=0).wait()
            service.executor.shutdown(wait=True)
