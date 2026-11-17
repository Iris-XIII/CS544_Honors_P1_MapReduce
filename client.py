"""Submit and monitor MapReduce jobs from outside the containers."""

import argparse
import math
from pathlib import Path
import sys
import time

# Generated modules use top-level imports; support `python client.py` directly.
sys.path.insert(0, str(Path(__file__).resolve().parent / "proto"))

import grpc
import boss_pb2 as pb
import boss_pb2_grpc


def task_count(value):
    count = int(value)
    if not 1 <= count <= 2**31 - 1:
        raise argparse.ArgumentTypeError("must be between 1 and 2147483647")
    return count


def positive_seconds(value):
    seconds = float(value)
    if not math.isfinite(seconds) or seconds <= 0:
        raise argparse.ArgumentTypeError("must be finite and greater than zero")
    return seconds


def shared_path(value):
    if not value.strip() or "\x00" in value or not Path(value).is_absolute():
        raise argparse.ArgumentTypeError("must be an absolute path visible to the Boss and Workers")
    # Do not resolve symlinks or check existence on the client's filesystem.
    return value


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--boss", default="localhost:50050", help="Boss host:port (default: localhost:50050)")
    parser.add_argument("--input", type=shared_path, help="shared input file path")
    parser.add_argument("--output", type=shared_path, help="shared final output directory")
    parser.add_argument("--job", type=shared_path, help="shared Python map/reduce file path")
    parser.add_argument("--maps", type=task_count, help="number of map tasks")
    parser.add_argument("--reduces", type=task_count, help="number of reduce tasks")
    parser.add_argument("--status", metavar="JOB_ID", help="monitor an existing job without submitting again")
    parser.add_argument("--no-wait", action="store_true", help="return after submission, or after one --status query")
    parser.add_argument("--poll-interval", type=positive_seconds, default=1.0, help="seconds between status queries (default: 1)")
    parser.add_argument("--rpc-timeout", type=positive_seconds, default=5.0, help="deadline for each RPC in seconds (default: 5)")
    parser.add_argument("--wait-timeout", type=positive_seconds, help="maximum monitoring time in seconds; does not cancel the job")
    args = parser.parse_args(argv)
    submission = ("input", "output", "job", "maps", "reduces")
    if not args.boss.strip():
        parser.error("--boss must be nonempty")
    if args.status is not None:
        if not args.status.strip():
            parser.error("--status requires a nonempty job ID")
        if any(getattr(args, field) is not None for field in submission):
            parser.error("--status cannot be combined with job-submission arguments")
    else:
        missing = [f"--{field}" for field in submission if getattr(args, field) is None]
        if missing:
            parser.error("submission requires " + ", ".join(missing))
    if args.no_wait and args.wait_timeout is not None:
        parser.error("--wait-timeout cannot be combined with --no-wait")
    return args


def monitor(stub, job_id, args):
    deadline = None if args.wait_timeout is None else time.monotonic() + args.wait_timeout
    previous = None
    while True:
        remaining = None if deadline is None else deadline - time.monotonic()
        if remaining is not None and remaining <= 0:
            raise TimeoutError
        timeout = args.rpc_timeout if remaining is None else min(args.rpc_timeout, remaining)
        try:
            status = stub.GetJobStatus(pb.JobStatusRequest(job_id=job_id), timeout=timeout)
        except grpc.RpcError:
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError from None
            raise
        if status.state not in (pb.JOB_QUEUED, pb.JOB_RUNNING, pb.JOB_COMPLETED, pb.JOB_FAILED):
            print(f"Error: Boss returned an unknown job state: {status.state}", file=sys.stderr)
            return 1
        progress = (status.state, status.completed_maps, status.num_maps,
                    status.completed_reducers, status.num_reducers)
        if progress != previous:
            state = pb.JobState.Name(status.state).removeprefix("JOB_")
            print(f"{state} | maps {status.completed_maps}/{status.num_maps}"
                  f" | reduces {status.completed_reducers}/{status.num_reducers}", flush=True)
            previous = progress
        if status.state == pb.JOB_COMPLETED:
            print(f"Output: {status.output_path}", flush=True)
            return 0
        if status.state == pb.JOB_FAILED:
            print(f"Job failed: {status.error or 'No error details provided'}", file=sys.stderr)
            return 1
        if args.no_wait:
            return 0
        delay = args.poll_interval
        if deadline is not None:
            delay = min(delay, max(0, deadline - time.monotonic()))
        time.sleep(delay)


def main(argv=None):
    args = parse_args(argv)
    job_id = args.status
    submitting = False
    try:
        with grpc.insecure_channel(args.boss) as channel:
            stub = boss_pb2_grpc.BossServiceStub(channel)
            if job_id is None:
                submitting = True
                reply = stub.SubmitJob(pb.SubmitJobRequest(
                    input_path=args.input, output_path=args.output, job_file=args.job,
                    num_maps=args.maps, num_reducers=args.reduces,
                ), timeout=args.rpc_timeout)
                job_id = reply.job_id
                submitting = False
                if not job_id:
                    print("Error: Boss returned an empty job ID", file=sys.stderr)
                    return 1
                print(f"Job ID: {job_id}", flush=True)
                if args.no_wait:
                    print("Submitted; execution may still be queued.", flush=True)
                    return 0
            else:
                print(f"Job ID: {job_id}", flush=True)
            return monitor(stub, job_id, args)
    except TimeoutError:
        print(f"Stopped waiting for job {job_id}; the job was not cancelled."
              " Resume with --status and the same job ID.", file=sys.stderr)
        return 124
    except grpc.RpcError as exc:
        print(f"Boss RPC error ({exc.code().name}): {exc.details()}", file=sys.stderr)
        if submitting and exc.code() in (
            grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.UNKNOWN, grpc.StatusCode.INTERNAL, grpc.StatusCode.CANCELLED,
        ):
            print("Submission was not retried. If the reply was lost, the job may"
                  " have been registered; retrying could create a duplicate.", file=sys.stderr)
        elif job_id:
            print(f"You can query job {job_id} again with --status.", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        detail = f" Job ID: {job_id}." if job_id else ""
        print(f"\nClient interrupted; no cancellation request was sent.{detail}", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
