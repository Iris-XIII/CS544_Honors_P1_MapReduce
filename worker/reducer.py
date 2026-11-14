
import importlib.util
import json
from collections import defaultdict
from pathlib import Path


def load_reduce_function(job_file):
    """Load a user-defined reduce() function."""
    spec = importlib.util.spec_from_file_location(
        "user_reduce_job", job_file
    )

    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load job file: {job_file}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if not callable(getattr(module, "reduce", None)):
        raise ValueError("Job file must define reduce()")

    return module.reduce


def execute_reduce(input_paths, job_file, output_path):
    """Group values by key and execute the user-defined reducer."""
    reduce_fn = load_reduce_function(job_file)

    grouped = defaultdict(list)

    # Read the intermediate files for this reduce partition.
    for input_path in input_paths:
        with open(input_path, encoding="utf-8") as source:
            for line in source:
                record = json.loads(line)
                key = record["key"]
                value = record["value"]

                # JSON keys can be lists/dicts, so serialize for grouping.
                stable_key = json.dumps(
                    key, sort_keys=True, ensure_ascii=False
                )

                if stable_key not in grouped:
                    grouped[stable_key] = {
                        "key": key,
                        "values": [],
                    }

                grouped[stable_key]["values"].append(value)

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    result_count = 0

    with open(output_path, "w", encoding="utf-8") as output:
        for item in grouped.values():
            result = reduce_fn(item["key"], item["values"])

            if result is None:
                continue

            key, value = result
            output.write(
                json.dumps(
                    {"key": key, "value": value},
                    ensure_ascii=False
                ) + "\n"
            )
            result_count += 1

    return result_count
