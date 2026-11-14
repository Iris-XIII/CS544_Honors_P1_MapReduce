
import importlib.util
import json
from pathlib import Path


def load_map_function(job_file):
    """Load the user-defined map() function from a Python file."""

    job_file = Path(job_file)

    spec = importlib.util.spec_from_file_location(
        "user_mapreduce_job", job_file
    )

    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load job file: {job_file}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if not callable(getattr(module, "map", None)):
        raise ValueError("Job file must define a map() function")

    return module.map


def execute_map(input_path, job_file, start_line, end_line, output_path):
    """
    Execute map() on lines in [start_line, end_line).

    Write emitted (key, value) pairs to a JSONL file.
    """

    if start_line < 0 or end_line < start_line:
        raise ValueError("Invalid input line range")

    map_fn = load_map_function(job_file)

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    emitted_count = 0

    with open(input_path, "r", encoding="utf-8") as source:
        with open(output_path, "w", encoding="utf-8") as output:

            for line_num, line in enumerate(source):
                if line_num >= end_line:
                    break

                if line_num < start_line:
                    continue

                record = line.rstrip("\r\n")

                for key, value in map_fn(line_num, record):
                    output.write(
                        json.dumps({"key": key, "value": value})
                        + "\n"
                    )
                    emitted_count += 1

    return emitted_count
