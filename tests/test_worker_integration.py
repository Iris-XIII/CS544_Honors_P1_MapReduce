
import json
import time
import tempfile
import unittest
from pathlib import Path

import grpc
import mapreduce_pb2 as pb2
import mapreduce_pb2_grpc as pb2_grpc


ROOT = Path(__file__).resolve().parents[1]


def wait_for_completion(stub, job_id, task_id, is_reduce):
    """Poll the Worker until a task completes or fails."""
    for _ in range(50):
        status = stub.GetTaskStatus(
            pb2.TaskStatusRequest(
                job_id=job_id,
                task_id=task_id,
                attempt_id=0,
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

    def test_map_reduce_over_grpc(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)

            input_path = ROOT / "examples/data/input.txt"
            job_file = ROOT / "examples/wordcount.py"

            channel = grpc.insecure_channel("localhost:50051")
            grpc.channel_ready_future(channel).result(timeout=5)
            stub = pb2_grpc.WorkerServiceStub(channel)

            # Submit two map tasks with non-overlapping line ranges.
            for task_id, (start, end) in enumerate([(0, 2), (2, 4)]):
                reply = stub.RunMap(
                    pb2.MapTaskRequest(
                        job_id="integration_test",
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

            map_outputs = []

            for task_id in range(2):
                status = wait_for_completion(
                    stub, "integration_test", task_id, False
                )
                map_outputs.append(Path(status.output_path))

            # Submit reduce tasks using each map's partition files.
            for reducer_id in range(2):
                input_paths = [
                    str(
                        path /
                        f"map_{map_id}_attempt_0_reduce_{reducer_id}.jsonl"
                    )
                    for map_id, path in enumerate(map_outputs)
                ]

                reply = stub.RunReduce(
                    pb2.ReduceTaskRequest(
                        job_id="integration_test",
                        task_id=reducer_id,
                        attempt_id=0,
                        job_file=str(job_file),
                        output_path=str(temp / "output"),
                        input_paths=input_paths,
                    ),
                    timeout=5,
                )
                self.assertTrue(reply.accepted)

            results = {}

            for reducer_id in range(2):
                status = wait_for_completion(
                    stub, "integration_test", reducer_id, True
                )

                with open(status.output_path, encoding="utf-8") as f:
                    for line in f:
                        record = json.loads(line)
                        results[record["key"]] = record["value"]

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

            channel.close()


if __name__ == "__main__":
    unittest.main()
