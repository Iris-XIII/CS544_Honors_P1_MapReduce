
import json
import tempfile
import unittest
from pathlib import Path

from worker.mapper import execute_map


class TestMapper(unittest.TestCase):

    def test_wordcount_partition(self):
        root = Path(__file__).resolve().parents[1]

        input_path = root / "examples/data/input.txt"
        job_file = root / "examples/wordcount.py"

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "map_output.jsonl"

            count = execute_map(
                input_path=input_path,
                job_file=job_file,
                start_line=0,
                end_line=2,
                output_path=output_path,
            )

            with open(output_path, encoding="utf-8") as f:
                records = [json.loads(line) for line in f]

            self.assertEqual(count, 4)

            self.assertEqual(
                records,
                [
                    {"key": "hello", "value": 1},
                    {"key": "world", "value": 1},
                    {"key": "hello", "value": 1},
                    {"key": "mapreduce", "value": 1},
                ],
            )


    def test_empty_partition(self):
        """An empty partition should produce no records."""
        root = Path(__file__).resolve().parents[1]

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "empty.jsonl"

            count = execute_map(
                input_path=root / "examples/data/input.txt",
                job_file=root / "examples/wordcount.py",
                start_line=2,
                end_line=2,
                output_path=output_path,
            )

            self.assertEqual(count, 0)
            self.assertEqual(output_path.read_text(), "")


    def test_invalid_partition(self):
        """Reject invalid input line ranges."""
        root = Path(__file__).resolve().parents[1]

        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaises(ValueError):
                execute_map(
                    input_path=root / "examples/data/input.txt",
                    job_file=root / "examples/wordcount.py",
                    start_line=5,
                    end_line=2,
                    output_path=Path(temp_dir) / "invalid.jsonl",
                )



if __name__ == "__main__":
    unittest.main()
