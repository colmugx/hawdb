# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Replay serial paired allocator probes and admitted-host controls."""

import json
from pathlib import Path
import subprocess
import sys


def main():
    directory = Path(sys.argv[1]).resolve()
    base = {
        "backend": "memory",
        "path": "unused",
        "insert_single": "CREATE (:Boundary {id: $id, score: $score, title: $title})",
        "insert_bulk": "UNWIND $rows AS row CREATE (:Boundary {id: row.id, score: row.score, title: row.title})",
        "point": "MATCH (n:Boundary) WHERE n.id = $id RETURN n.id AS id, n.score AS score, n.title AS title",
        "scan": "MATCH (n:Boundary) RETURN n.id AS id, n.score AS score, n.title AS title",
        "columns": ["id", "score", "title"],
    }
    for case, count in [("point", 128), ("select", 4096), ("fill_bulk", 512)]:
        job = base | {
            "case": case,
            "rows": [
                {"id": index, "score": index * 0.5, "title": f"Synthetic row {index}"}
                for index in range(count)
            ],
        }
        (directory / f"{case}.json").write_text(json.dumps(job) + "\n")
    with (directory / "replay-host.jsonl").open("w") as output:
        for sample in range(7):
            for case in ["point", "select", "fill_bulk"]:
                for mode in ["timing", "allocations"]:
                    for label in labels(sample):
                        data = run(directory / f"{label}-{mode}", directory / f"{case}.json")
                        assert data["status"] == "ok", data
                        write(output, data, sample=sample, mode=mode, label=label)
    with (directory / "replay-facade.jsonl").open("w") as output:
        for sample in range(3):
            for mode in ["ordinary", "concurrent"]:
                for case in ["point", "scan", "update"]:
                    for label in labels(sample):
                        data = run(directory / f"{label}-probe", mode, case)
                        write(output, data, sample=sample, label=label)


def labels(sample):
    return ["baseline", "candidate"] if sample % 2 == 0 else ["candidate", "baseline"]


def run(*arguments):
    result = subprocess.run(arguments, check=True, capture_output=True, text=True)
    return json.loads(result.stdout)


def write(output, data, **metadata):
    output.write(json.dumps(data | metadata) + "\n")


if __name__ == "__main__":
    main()
