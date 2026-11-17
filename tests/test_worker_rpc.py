
import json
import tempfile
import unittest
import uuid
from pathlib import Path

import mapreduce_pb2 as pb2

from worker_test_utils import running_worker
from test_worker_integration import wait_for_completion


ROOT = Path(__file__).resolve().parents[1]


class TestWorkerRPC(unittest.TestCase):

    def setUp(self):
        """Start a temporary Worker server for each test."""

        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)

        self.shared_root = Path(self.temp_dir.name)

        worker_context = running_worker(self.shared_root)
        self.stub = worker_context.__enter__()

        # Stop the Worker before removing temporary files.
        self.addCleanup(
            lambda: worker_context.__exit__(None, None, None)
        )

    def test_worker_rpc(self):
        """Verify Worker RPC submission, execution, and status."""

        stub = self.stub
        job_id = f"rpc_test_{uuid.uuid4().hex}"

        input_path = ROOT / "examples/data/input.txt"
        job_file = ROOT / "examples/wordcount.py"

        # --------------------------------------------------
        # 1. Submit a valid map task.
        # --------------------------------------------------

        map_response = stub.RunMap(
            pb2.MapTaskRequest(
                job_id=job_id,
                task_id=0,
                attempt_id=0,
                input_path=str(input_path),
                job_file=str(job_file),
                num_reducers=2,
                start_line=0,
                end_line=2,
            ),
            timeout=5,
        )

        self.assertTrue(map_response.accepted)

        # --------------------------------------------------
        # 2. Verify the initial task status.
        # --------------------------------------------------

        status = stub.GetTaskStatus(
            pb2.TaskStatusRequest(
                job_id=job_id,
                task_id=0,
                attempt_id=0,
                is_reduce=False,
            ),
            timeout=5,
        )

        # The Worker executes asynchronously, so its state
        # may already have advanced beyond PENDING.
        self.assertIn(
            status.state,
            (
                pb2.PENDING,
                pb2.RUNNING,
                pb2.COMPLETED,
            ),
        )

        # --------------------------------------------------
        # 3. Verify duplicate attempts are rejected.
        # --------------------------------------------------

        duplicate = stub.RunMap(
            pb2.MapTaskRequest(
                job_id=job_id,
                task_id=0,
                attempt_id=0,
                input_path=str(input_path),
                job_file=str(job_file),
                num_reducers=2,
                start_line=0,
                end_line=2,
            ),
            timeout=5,
        )

        self.assertFalse(duplicate.accepted)

        # --------------------------------------------------
        # 4. Wait for the map task to finish.
        # --------------------------------------------------

        map_status = wait_for_completion(
            stub,
            job_id,
            task_id=0,
            is_reduce=False,
        )

        map_output = Path(map_status.output_path)
        self.assertTrue(map_output.is_dir())

        # --------------------------------------------------
        # 5. Submit a valid reduce task.
        # --------------------------------------------------

        partition_path = map_output / (
            "map_0_attempt_0_reduce_0.jsonl"
        )

        self.assertTrue(partition_path.is_file())

        output_dir = self.shared_root / "output"

        reduce_response = stub.RunReduce(
            pb2.ReduceTaskRequest(
                job_id=job_id,
                task_id=0,
                attempt_id=0,
                job_file=str(job_file),
                output_path=str(output_dir),
                input_paths=[str(partition_path)],
            ),
            timeout=5,
        )

        self.assertTrue(reduce_response.accepted)

        # --------------------------------------------------
        # 6. Wait for the reduce task to finish.
        # --------------------------------------------------

        reduce_status = wait_for_completion(
            stub,
            job_id,
            task_id=0,
            is_reduce=True,
        )

        result_path = Path(reduce_status.output_path)
        self.assertTrue(result_path.is_file())

        with result_path.open(encoding="utf-8") as f:
            results = [
                json.loads(line)
                for line in f
            ]

        for record in results:
            self.assertIn("key", record)
            self.assertIn("value", record)

        # --------------------------------------------------
        # 7. Verify unknown tasks return UNKNOWN.
        # --------------------------------------------------

        unknown = stub.GetTaskStatus(
            pb2.TaskStatusRequest(
                job_id="missing_job",
                task_id=99,
                attempt_id=0,
                is_reduce=False,
            ),
            timeout=5,
        )

        self.assertEqual(unknown.state, pb2.UNKNOWN)


if __name__ == "__main__":
    unittest.main()
