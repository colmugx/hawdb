#!/usr/bin/env python3
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Replay serial source-bound facade probes and admitted-host controls."""

import json
from pathlib import Path
import subprocess
import sys


def run(*arguments):
    result = subprocess.run(arguments, check=True, capture_output=True, text=True)
    return json.loads(result.stdout)


def labels(sample):
    return ["baseline", "candidate"] if sample % 2 == 0 else ["candidate", "baseline"]


def main():
    binaries = Path(sys.argv[1]).resolve()
    output = Path(sys.argv[2]).resolve()
    output.mkdir(parents=True, exist_ok=False)
    with (output / "component.jsonl").open("w") as stream:
        subprocess.run([str(binaries / "candidate-component"), "1000"], check=True, stdout=stream)
    with (output / "facade.jsonl").open("w") as stream:
        for sample in range(3):
            for mode in ["ordinary", "concurrent"]:
                for case in ["point", "scan", "update"]:
                    pair = []
                    for label in labels(sample):
                        data = run(binaries / f"{label}-probe", mode, case)
                        pair.append(data)
                        stream.write(json.dumps(data | {"label": label, "sample": sample}) + "\n")
                    assert pair[0]["calls"] == pair[1]["calls"]
                    assert (pair[0]["live_bytes_after"] - pair[0]["live_bytes_before"] ==
                            pair[1]["live_bytes_after"] - pair[1]["live_bytes_before"])
    common = {
        "backend": "memory", "path": "unused",
        "insert_single": "CREATE (:Boundary {id: $id, score: $score, title: $title})",
        "insert_bulk": "UNWIND $rows AS row CREATE (:Boundary {id: row.id, score: row.score, title: row.title})",
        "point": "MATCH (n:Boundary) WHERE n.id = $id RETURN n.id AS id, n.score AS score, n.title AS title",
        "scan": "MATCH (n:Boundary) RETURN n.id AS id, n.score AS score, n.title AS title",
        "columns": ["id", "score", "title"],
    }
    for fixture, case, count in [("point_one", "point", 1), ("point", "point", 128),
                                 ("select", "select", 4096), ("fill_bulk", "fill_bulk", 512)]:
        job = common | {"case": case, "rows": [
            {"id": i, "score": i * 0.5, "title": f"Synthetic row {i}"} for i in range(count)
        ]}
        (output / f"{fixture}.json").write_text(json.dumps(job))
    signatures = {}
    with (output / "paired.jsonl").open("w") as stream:
        for sample in range(7):
            for fixture in ["point_one", "point", "select", "fill_bulk"]:
                for mode in ["timing", "allocations"]:
                    for label in labels(sample):
                        data = run(binaries / f"{label}-{mode}", output / f"{fixture}.json")
                        assert data["status"] == "ok", data
                        signature = {key: data[key] for key in [
                            "backend", "case", "checksum", "durability", "input_rows",
                            "output_rows", "values", "engine_payload_bytes", "prefetch",
                        ]}
                        assert signatures.setdefault(fixture, signature) == signature
                        metadata = {"label": label, "sample": sample, "fixture": fixture, "mode": mode}
                        stream.write(json.dumps(data | metadata) + "\n")
    print(f"Replay artifacts: {output}")


if __name__ == "__main__":
    main()
