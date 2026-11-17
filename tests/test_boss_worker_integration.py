"""Self-contained Boss-to-Worker checks; require the executing Worker from PR #6.

No manually started service or fixed port is required. On the old Worker skeleton
these tests are explicitly skipped; all scheduler contract tests still run.
"""

from concurrent import futures
import json
from pathlib import Path
import tempfile
from threading import Event
from time import monotonic
import unittest
from unittest.mock import patch

import grpc

import boss_pb2
import boss_pb2_grpc
import mapreduce_pb2_grpc
from boss.jobs import JobStore
from boss.scheduler import Scheduler
from boss.server import create_server
from worker import worker as worker_module


ROOT = Path(__file__).resolve().parents[1]


@unittest.skipUnless(
    hasattr(worker_module.WorkerService, "_execute_map"),
    "Requires the executing Worker from PR #6",
)
class TestBossWorkerIntegration(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.shared = self.root / "shared"
        shared_patch = patch.object(worker_module, "SHARED_ROOT", self.shared)
        shared_patch.start()
        self.addCleanup(shared_patch.stop)
        self.workers = []
        addresses = []
        for _ in range(4):
            worker = worker_module.WorkerService(max_tasks=1)
            self.addCleanup(worker.executor.shutdown, wait=True, cancel_futures=True)
            server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
            mapreduce_pb2_grpc.add_WorkerServiceServicer_to_server(worker, server)
            port = server.add_insecure_port("127.0.0.1:0")
            server.start()
            self.addCleanup(lambda server=server: server.stop(0).wait())
            addresses.append(f"127.0.0.1:{port}")
            self.workers.append(worker)

        self.store = JobStore()
        server, port = create_server("127.0.0.1:0", self.store)
        server.start()
        self.addCleanup(lambda: server.stop(0).wait())
        channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        self.addCleanup(channel.close)
        grpc.channel_ready_future(channel).result(timeout=5)
        self.stub = boss_pb2_grpc.BossServiceStub(channel)
        # Separate scheduler and Worker workspaces are valid shared locations.
        self.scheduler = Scheduler(
            self.store, addresses, self.shared / "scheduler",
            poll_interval=0.01, rpc_timeout=2, task_timeout=10,
        )
        self.addCleanup(self.scheduler.stop)

    def submit(self, name, maps, reduces, input_path=None, job_file=None):
        return self.stub.SubmitJob(boss_pb2.SubmitJobRequest(
            input_path=str(input_path or ROOT / "examples/data/input.txt"),
            output_path=str(self.root / name),
            job_file=str(job_file or ROOT / "examples/wordcount.py"),
            num_maps=maps, num_reducers=reduces,
        ), timeout=5).job_id

    def results(self, job_id, maps, reduces):
        deadline = monotonic() + 15
        while monotonic() < deadline:
            status = self.stub.GetJobStatus(
                boss_pb2.JobStatusRequest(job_id=job_id), timeout=5
            )
            if status.state in (boss_pb2.JOB_COMPLETED, boss_pb2.JOB_FAILED):
                break
            Event().wait(0.01)
        else:
            self.fail("Job did not reach a terminal state")
        self.assertEqual(status.state, boss_pb2.JOB_COMPLETED, status.error)
        self.assertEqual((status.completed_maps, status.completed_reducers), (maps, reduces))
        parts = sorted(Path(status.output_path).iterdir())
        self.assertEqual([p.name for p in parts], [f"part-{i:05d}.jsonl" for i in range(reduces)])
        records = {}
        for part in parts:
            for line in part.read_text().splitlines():
                record = json.loads(line)
                self.assertNotIn(record["key"], records, "A key appeared in multiple reducer outputs")
                records[record["key"]] = record["value"]
        return records

    def test_wordcount_across_task_counts_and_four_workers(self):
        expected = {"hello": 2, "world": 3, "mapreduce": 1, "distributed": 1, "computing": 1}
        self.scheduler.start()
        for maps, reduces in ((1, 1), (4, 2), (8, 4)):
            with self.subTest(maps=maps, reduces=reduces):
                job_id = self.submit(f"output-{maps}-{reduces}", maps, reduces)
                self.assertEqual(self.results(job_id, maps, reduces), expected)
        for worker in self.workers:
            with worker.lock:
                self.assertTrue(worker.tasks, "Every Worker should receive work")

    def test_empty_input_produces_empty_reducer_files(self):
        input_path = self.root / "empty.txt"
        input_path.write_text("")
        job_id = self.submit("empty-output", 5, 3, input_path=input_path)
        self.scheduler.start()
        self.assertEqual(self.results(job_id, 5, 3), {})

    def test_custom_map_reduce_functions(self):
        input_path = self.root / "numbers.txt"
        input_path.write_text("1\n2\n3\n4\n5\n")
        job_file = self.root / "sum_by_parity.py"
        job_file.write_text(
            "def map(key, value):\n"
            "    number = int(value)\n"
            "    yield number % 2, number\n\n"
            "def reduce(key, values):\n"
            "    return key, sum(values)\n"
        )
        job_id = self.submit("sums", 3, 2, input_path=input_path, job_file=job_file)
        self.scheduler.start()
        self.assertEqual(self.results(job_id, 3, 2), {0: 6, 1: 9})

    def test_unselected_attempt_output_is_not_consumed(self):
        job_id = self.submit("selected-output", 2, 1)
        orphan = self.shared / "intermediate" / job_id / "map_0_attempt_1"
        orphan.mkdir(parents=True)
        (orphan / "map_0_attempt_1_reduce_0.jsonl").write_text(
            '{"key": "hello", "value": 1000}\n'
        )
        self.scheduler.start()
        self.assertEqual(self.results(job_id, 2, 1), {
            "hello": 2, "world": 3, "mapreduce": 1, "distributed": 1, "computing": 1,
        })


if __name__ == "__main__":
    unittest.main()
