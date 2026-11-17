"""Run the CLI as a separate process against isolated gRPC services."""

from concurrent import futures
from contextlib import redirect_stderr, redirect_stdout
import io
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import grpc
import boss_pb2
import mapreduce_pb2_grpc
import client
from boss.jobs import JobStore
from boss.scheduler import Scheduler
from boss.server import create_server
from worker import worker as worker_module


ROOT = Path(__file__).resolve().parents[1]


class TestClient(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        shared_patch = patch.object(worker_module, "SHARED_ROOT", self.root / "shared")
        shared_patch.start()
        self.addCleanup(shared_patch.stop)
        addresses = []
        for _ in range(2):
            worker = worker_module.WorkerService()
            self.addCleanup(worker.executor.shutdown, wait=True, cancel_futures=True)
            server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
            mapreduce_pb2_grpc.add_WorkerServiceServicer_to_server(worker, server)
            port = server.add_insecure_port("127.0.0.1:0")
            server.start()
            self.addCleanup(lambda server=server: server.stop(0).wait())
            addresses.append(f"127.0.0.1:{port}")
        self.store = JobStore()
        server, port = create_server("127.0.0.1:0", self.store)
        server.start()
        self.addCleanup(lambda: server.stop(0).wait())
        self.address = f"127.0.0.1:{port}"
        self.scheduler = Scheduler(self.store, addresses, self.root / "work", poll_interval=0.01)
        self.addCleanup(self.scheduler.stop)

    def submission(self, **overrides):
        settings = {
            "input": str(ROOT / "examples/data/input.txt"),
            "output": str(self.root / "output"),
            "job": str(ROOT / "examples/wordcount.py"),
            "maps": "4", "reduces": "2",
        }
        settings.update(overrides)
        return [item for key, value in settings.items() for item in (f"--{key}", value)]

    def cli(self, *args):
        env = os.environ.copy()
        env.pop("PYTHONPATH", None)
        return subprocess.run(
            [sys.executable, str(ROOT / "client.py"), "--boss", self.address,
             "--rpc-timeout", "2", "--poll-interval", "0.01", *args],
            cwd=self.root, env=env, capture_output=True, text=True, timeout=15,
        )

    def job_id(self, result):
        return next(line.removeprefix("Job ID: ") for line in result.stdout.splitlines()
                    if line.startswith("Job ID: "))

    def test_wordcount_through_cli(self):
        self.scheduler.start()
        result = self.cli(*self.submission(), "--wait-timeout", "10")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("COMPLETED | maps 4/4 | reduces 2/2", result.stdout)
        self.assertIn(f"Output: {self.root / 'output'}", result.stdout)
        records = {}
        for part in (self.root / "output").glob("part-*.jsonl"):
            for line in part.read_text().splitlines():
                record = json.loads(line)
                self.assertNotIn(record["key"], records)
                records[record["key"]] = record["value"]
        self.assertEqual(records, {"hello": 2, "world": 3, "mapreduce": 1,
                                   "distributed": 1, "computing": 1})

    def test_submit_then_resume_without_creating_a_second_job(self):
        submitted = self.cli(*self.submission(), "--no-wait")
        self.assertEqual(submitted.returncode, 0, submitted.stderr)
        job_id = self.job_id(submitted)
        snapshot = self.cli("--status", job_id, "--no-wait")
        self.assertEqual(snapshot.returncode, 0, snapshot.stderr)
        self.assertIn("QUEUED | maps 0/4 | reduces 0/2", snapshot.stdout)
        self.scheduler.start()
        resumed = self.cli("--status", job_id, "--wait-timeout", "10")
        self.assertEqual(resumed.returncode, 0, resumed.stderr)
        self.assertEqual(self.job_id(resumed), job_id)
        self.assertIsNone(self.store.claim_next())

    def test_wait_timeout_leaves_job_queued(self):
        result = self.cli(*self.submission(), "--wait-timeout", "0.15")
        self.assertEqual(result.returncode, 124, result.stderr)
        self.assertIn("not cancelled", result.stderr)
        self.assertEqual(result.stdout.count("QUEUED |"), 1)
        self.assertEqual(self.store.get(self.job_id(result)).state, boss_pb2.JOB_QUEUED)

    def test_worker_failure_is_reported_with_nonzero_exit(self):
        job_file = self.root / "broken.py"
        job_file.write_text("def map(key, value):\n    raise ValueError('bad input record')\n")
        self.scheduler.start()
        result = self.cli(*self.submission(job=str(job_file)), "--wait-timeout", "10")
        self.assertEqual(result.returncode, 1)
        self.assertIn("FAILED", result.stdout)
        self.assertIn("bad input record", result.stderr)

    def test_unknown_job_reports_not_found(self):
        result = self.cli("--status", "missing")
        self.assertEqual(result.returncode, 1)
        self.assertIn("NOT_FOUND", result.stderr)
        self.assertNotIn("Traceback", result.stderr)

    def test_missing_input_is_validated_by_boss(self):
        result = self.cli(*self.submission(input=str(self.root / "missing")), "--no-wait")
        self.assertEqual(result.returncode, 1)
        self.assertIn("INVALID_ARGUMENT", result.stderr)

    def test_unreachable_boss_reports_rpc_error(self):
        with socket.socket() as unused:
            unused.bind(("127.0.0.1", 0))
            result = self.cli("--status", "anything", "--boss",
                              f"127.0.0.1:{unused.getsockname()[1]}")
        self.assertEqual(result.returncode, 1)
        self.assertIn("UNAVAILABLE", result.stderr)

    def test_invalid_arguments(self):
        cases = [[], ["--status", ""], ["--status", "id", "--maps", "1"],
                 [*self.submission(maps="0")], [*self.submission(reduces="2147483648")],
                 [*self.submission(input="relative.txt")],
                 [*self.submission(), "--poll-interval", "nan"],
                 [*self.submission(), "--rpc-timeout", "-1"],
                 [*self.submission(), "--no-wait", "--wait-timeout", "1"]]
        for args in cases:
            with self.subTest(args=args):
                result = self.cli(*args)
                self.assertEqual(result.returncode, 2)
                self.assertIn("error:", result.stderr)

    def test_shared_paths_are_not_resolved_on_client(self):
        args = client.parse_args(self.submission(input="/shared/input/data.txt"))
        self.assertEqual(args.input, "/shared/input/data.txt")

    def test_interrupt_does_not_cancel_job(self):
        submitted = self.cli(*self.submission(), "--no-wait")
        job_id = self.job_id(submitted)
        error = io.StringIO()
        with patch.object(client.time, "sleep", side_effect=KeyboardInterrupt), \
                redirect_stdout(io.StringIO()), redirect_stderr(error):
            code = client.main(["--boss", self.address, "--status", job_id])
        self.assertEqual(code, 130)
        self.assertIn("no cancellation request", error.getvalue())
        self.assertEqual(self.store.get(job_id).state, boss_pb2.JOB_QUEUED)


if __name__ == "__main__":
    unittest.main()
