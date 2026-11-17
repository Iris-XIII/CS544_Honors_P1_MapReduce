"""Run with PYTHONPATH=proto python -m boss.server from the repository root."""

import argparse
from concurrent import futures
from pathlib import Path

import grpc

import boss_pb2
import boss_pb2_grpc
from boss.jobs import JobSpec, JobStore


def validate_submission(request) -> JobSpec:
    """Check metadata without importing or executing user-provided Python."""
    if request.num_maps <= 0:
        raise ValueError("num_maps must be positive")
    if request.num_reducers <= 0:
        raise ValueError("num_reducers must be positive")

    paths = {}
    for name in ("input_path", "output_path", "job_file"):
        value = getattr(request, name)
        if not value.strip() or "\x00" in value:
            raise ValueError(f"{name} must be a nonempty path without null bytes")
        path = Path(value)
        if not path.is_absolute():
            raise ValueError(f"{name} must be absolute and visible to the Boss and Workers")
        paths[name] = path

    for name in ("input_path", "job_file"):
        path = paths[name]
        try:
            if not path.is_file():
                raise ValueError(f"{name} must refer to an existing regular file")
            # Opening checks readability without reading a potentially large input.
            with path.open("rb"):
                pass
        except OSError as exc:
            raise ValueError(f"{name} cannot be read: {exc.strerror}") from exc

    # Accept an existing directory or one that can be created under a directory.
    # Do not create output files/directories until execution starts.
    try:
        for path in (paths["output_path"], *paths["output_path"].parents):
            if path.is_symlink() and not path.exists():
                raise ValueError("output_path contains a dangling symlink")
            if path.exists() and not path.is_dir():
                raise ValueError("output_path and its existing parents must be directories")
    except OSError as exc:
        raise ValueError(f"output_path cannot be inspected: {exc.strerror}") from exc

    return JobSpec(
        input_path=str(paths["input_path"]),
        output_path=str(paths["output_path"]),
        job_file=str(paths["job_file"]),
        num_maps=request.num_maps,
        num_reducers=request.num_reducers,
    )


class BossService(boss_pb2_grpc.BossServiceServicer):
    def __init__(self, store=None):
        self.store = store if store is not None else JobStore()

    def SubmitJob(self, request, context):
        try:
            spec = validate_submission(request)
        except ValueError as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))

        record = self.store.create(spec)
        return boss_pb2.SubmitJobReply(job_id=record.job_id, state=record.state)

    def GetJobStatus(self, request, context):
        if not request.job_id.strip():
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "job_id must be nonempty")
        try:
            record = self.store.get(request.job_id)
        except KeyError:
            context.abort(grpc.StatusCode.NOT_FOUND, "Job not found")

        return boss_pb2.JobStatusReply(
            job_id=record.job_id,
            state=record.state,
            num_maps=record.spec.num_maps,
            num_reducers=record.spec.num_reducers,
            completed_maps=record.completed_maps,
            completed_reducers=record.completed_reducers,
            output_path=record.spec.output_path,
            error=record.error,
            submitted_at_unix_seconds=record.submitted_at_unix_seconds,
        )


def create_server(address="[::]:50050", store=None):
    """Create a bound server; callers start it. Port 0 selects a free test port."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    boss_pb2_grpc.add_BossServiceServicer_to_server(BossService(store), server)
    port = server.add_insecure_port(address)
    if port == 0:
        raise RuntimeError(f"Failed to bind Boss gRPC address {address}")
    return server, port


def main():
    parser = argparse.ArgumentParser(description="MapReduce Boss gRPC server")
    parser.add_argument("--address", default="[::]:50050", help="gRPC bind address")
    args = parser.parse_args()
    server, port = create_server(args.address)
    server.start()
    print(f"[Boss] Listening on {args.address} (port {port})", flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=2).wait()


if __name__ == "__main__":
    main()
