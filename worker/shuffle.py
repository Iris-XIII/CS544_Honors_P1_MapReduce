
import hashlib
import json
from pathlib import Path


def get_partition(key, num_reducers):
    """Assign a key to a reducer using a stable hash."""
    if num_reducers <= 0:
        raise ValueError("num_reducers must be positive")

    encoded = json.dumps(
        key, sort_keys=True, ensure_ascii=False
    ).encode("utf-8")

    hash_value = int.from_bytes(
        #SHA-256 is deterministic
        hashlib.sha256(encoded).digest(), "big"
    )

    return hash_value % num_reducers


def partition_intermediate(
    input_path, output_dir, task_id, attempt_id, num_reducers
):
    """Split mapper output into reducer-specific JSONL files."""
    if num_reducers <= 0:
        raise ValueError("num_reducers must be positive")

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    paths = [
        output_dir / (
            f"map_{task_id}_attempt_{attempt_id}_"
            f"reduce_{r}.jsonl"
        )
        for r in range(num_reducers)
    ]

    # Open all reducer partition files.
    files = [open(p, "w", encoding="utf-8") for p in paths]

    try:
        with open(input_path, encoding="utf-8") as source:
            for line in source:
                record = json.loads(line)
                reducer_id = get_partition(
                    record["key"], num_reducers
                )
                files[reducer_id].write(
                    json.dumps(record, ensure_ascii=False) + "\n"
                )
    finally:
        for f in files:
            f.close()

    return paths
