
import json
import time
import tempfile
import unittest
import uuid
from pathlib import Path

import mapreduce_pb2 as pb2
from worker_test_utils import running_worker

from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]


def wait_for_completion(stub, job_id, task_id, is_reduce, attempt_id=0):
    """Poll the Worker until a task completes or fails."""
    for _ in range(50):
        status = stub.GetTaskStatus(
            pb2.TaskStatusRequest(
                job_id=job_id,
                task_id=task_id,
                attempt_id=attempt_id,
                is_reduce=is_reduce,
            ),
            timeout=5,
        )

        if status.state == pb2.COMPLETED:
            return status

        if status.state == pb2.FAILED:
            raise AssertionError(
                f"Task failed: {status.error}"
            )

        time.sleep(0.1)

    raise TimeoutError("Task did not complete within 5 seconds")


class TestWorkerIntegration(unittest.TestCase):

    def setUp(self):
        """Create temporary storage and start an isolated Worker server."""

        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)

        self.shared_root = Path(self.temp_dir.name)

        worker_context = running_worker(self.shared_root)
        self.stub = worker_context.__enter__()

        # Shut down the Worker before deleting temporary storage.
        self.addCleanup(
            lambda: worker_context.__exit__(None, None, None)
        )


    
    def test_configurable_map_straggler_delay(self):
        """Only the configured map task and attempt should be delayed."""

        from worker import worker as worker_module

        input_path = ROOT / "examples/data/input.txt"
        job_file = ROOT / "examples/wordcount.py"

        # Use a mocked sleep to avoid an actual 8-second wait.
        with (
            patch.dict(
                "os.environ",
                {
                    "MAP_DELAY_SECONDS": "8",
                    "MAP_DELAY_TASK_ID": "0",
                    "MAP_DELAY_ATTEMPT_ID": "0",
                },
            ),
            patch.object(worker_module.time, "sleep") as mock_sleep,
        ):
            job_id = f"delay_{uuid.uuid4().hex}"

            # Attempt 0 of map task 0 should be delayed.
            # Attempt 1 of the same task should not.
            # Map task 1 should not be delayed.
            cases = [
                (0, 0),
                (0, 1),
                (1, 0),
            ]

            for task_id, attempt_id in cases:
                response = self.stub.RunMap(
                    pb2.MapTaskRequest(
                        job_id=job_id,
                        task_id=task_id,
                        attempt_id=attempt_id,
                        input_path=str(input_path),
                        job_file=str(job_file),
                        num_reducers=2,
                        start_line=0,
                        end_line=2,
                    ),
                    timeout=5,
                )
                self.assertTrue(response.accepted)

            for task_id, attempt_id in cases:
                status = wait_for_completion(
                    self.stub,
                    job_id,
                    task_id,
                    is_reduce=False,
                    attempt_id=attempt_id,
                )
                self.assertTrue(Path(status.output_path).is_dir())

            # Only the matching task/attempt should sleep.
            mock_sleep.assert_any_call(8.0)

            delay_calls = [
                call
                for call in mock_sleep.call_args_list
                if call.args == (8.0,)
            ]
            self.assertEqual(len(delay_calls), 1)


    def test_map_reduce_over_grpc(self):
        """Verify the complete map -> shuffle -> reduce pipeline."""

        stub = self.stub
        job_id = f"integration_{uuid.uuid4().hex}"

        input_path = ROOT / "examples/data/input.txt"
        job_file = ROOT / "examples/wordcount.py"

        output_dir = self.shared_root / "output"

        # --------------------------------------------------
        # Step 1: Submit two map tasks.
        # --------------------------------------------------

        for task_id, (start, end) in enumerate([(0, 2), (2, 4)]):
            reply = stub.RunMap(
                pb2.MapTaskRequest(
                    job_id=job_id,
                    task_id=task_id,
                    attempt_id=0,
                    input_path=str(input_path),
                    job_file=str(job_file),
                    num_reducers=2,
                    start_line=start,
                    end_line=end,
                ),
                timeout=5,
            )

            self.assertTrue(reply.accepted)

        # --------------------------------------------------
        # Step 2: Wait for map tasks to complete.
        # --------------------------------------------------

        map_outputs = []

        for task_id in range(2):
            status = wait_for_completion(
                stub,
                job_id,
                task_id,
                is_reduce=False,
            )

            output_path = Path(status.output_path)
            self.assertTrue(output_path.is_dir())

            map_outputs.append(output_path)

        # --------------------------------------------------
        # Step 3: Submit two reduce tasks.
        # --------------------------------------------------

        for reducer_id in range(2):
            input_paths = [
                str(
                    path /
                    f"map_{map_id}_attempt_0_reduce_{reducer_id}.jsonl"
                )
                for map_id, path in enumerate(map_outputs)
            ]

            # Verify that the map partition files exist.
            for input_file in input_paths:
                self.assertTrue(Path(input_file).is_file())

            reply = stub.RunReduce(
                pb2.ReduceTaskRequest(
                    job_id=job_id,
                    task_id=reducer_id,
                    attempt_id=0,
                    job_file=str(job_file),
                    output_path=str(output_dir),
                    input_paths=input_paths,
                ),
                timeout=5,
            )

            self.assertTrue(reply.accepted)

        # --------------------------------------------------
        # Step 4: Wait for reduce tasks and collect results.
        # --------------------------------------------------

        results = {}

        for reducer_id in range(2):
            status = wait_for_completion(
                stub,
                job_id,
                reducer_id,
                is_reduce=True,
            )

            result_path = Path(status.output_path)
            self.assertTrue(result_path.is_file())

            with result_path.open(encoding="utf-8") as f:
                for line in f:
                    record = json.loads(line)
                    results[record["key"]] = record["value"]

        # --------------------------------------------------
        # Step 5: Verify final word counts.
        # --------------------------------------------------

        self.assertEqual(
            results,
            {
                "hello": 2,
                "world": 3,
                "mapreduce": 1,
                "distributed": 1,
                "computing": 1,
            },
        )

    def test_map_attempt_isolation(self):
        """Different attempts must not overwrite each other's output."""

        stub = self.stub
        job_id = f"isolation_{uuid.uuid4().hex}"

        input_path = ROOT / "examples/data/input.txt"
        job_file = ROOT / "examples/wordcount.py"

        output_paths = []

        # --------------------------------------------------
        # Step 1: Submit two attempts of the same map task.
        # --------------------------------------------------

        for attempt_id in (0, 1):
            reply = stub.RunMap(
                pb2.MapTaskRequest(
                    job_id=job_id,
                    task_id=0,
                    attempt_id=attempt_id,
                    input_path=str(input_path),
                    job_file=str(job_file),
                    num_reducers=2,
                    start_line=0,
                    end_line=2,
                ),
                timeout=5,
            )

            self.assertTrue(reply.accepted)

        # --------------------------------------------------
        # Step 2: Wait for both attempts to complete.
        # --------------------------------------------------

        for attempt_id in (0, 1):
            status = wait_for_completion(
                stub,
                job_id,
                task_id=0,
                is_reduce=False,
                attempt_id=attempt_id,
            )

            output_path = Path(status.output_path)
            self.assertTrue(output_path.is_dir())

            output_paths.append(output_path)

        # --------------------------------------------------
        # Step 3: Verify separate output directories.
        # --------------------------------------------------

        self.assertEqual(len(output_paths), 2)
        self.assertNotEqual(output_paths[0], output_paths[1])

        # Both directories must belong to the temporary storage.
        for output_path in output_paths:
            self.assertTrue(
                output_path.is_relative_to(self.shared_root)
            )

        # --------------------------------------------------
        # Step 4: Verify each attempt's partition files.
        # --------------------------------------------------

        for attempt_id, output_dir in enumerate(output_paths):
            for reducer_id in range(2):
                partition = output_dir / (
                    f"map_0_attempt_{attempt_id}_"
                    f"reduce_{reducer_id}.jsonl"
                )

                self.assertTrue(
                    partition.is_file(),
                    f"Missing partition: {partition}",
                )

                # Verify that each line is valid JSONL.
                with partition.open(encoding="utf-8") as f:
                    for line in f:
                        record = json.loads(line)
                        self.assertIn("key", record)
                        self.assertIn("value", record)


if __name__ == "__main__":
    unittest.main()
