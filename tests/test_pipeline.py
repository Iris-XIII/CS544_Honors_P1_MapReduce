
import json
import tempfile
import unittest
from pathlib import Path

from worker.mapper import execute_map
from worker.shuffle import partition_intermediate
from worker.reducer import execute_reduce


class TestMapReducePipeline(unittest.TestCase):

    def test_wordcount_pipeline(self):
        root = Path(__file__).resolve().parents[1]

        input_path = root / "examples/data/input.txt"
        job_file = root / "examples/wordcount.py"

        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            intermediate = temp / "intermediate"
            intermediate.mkdir()

            # Two map tasks process distinct input partitions.
            map_ranges = [(0, 2), (2, 4)]
            partitions = []

            for task_id, (start, end) in enumerate(map_ranges):
                map_output = temp / f"map_{task_id}.jsonl"

                execute_map(
                    input_path, job_file, start, end, map_output
                )

                paths = partition_intermediate(
                    input_path=map_output,
                    output_dir=intermediate,
                    task_id=task_id,
                    attempt_id=0,
                    num_reducers=2,
                )
                partitions.append(paths)

            results = {}

            # Each reducer reads its partition from both mappers.
            for reducer_id in range(2):
                inputs = [
                    partitions[map_id][reducer_id]
                    for map_id in range(2)
                ]

                output_path = temp / f"reduce_{reducer_id}.jsonl"

                execute_reduce(
                    inputs, job_file, output_path
                )

                with open(output_path) as f:
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


if __name__ == "__main__":
    unittest.main()
