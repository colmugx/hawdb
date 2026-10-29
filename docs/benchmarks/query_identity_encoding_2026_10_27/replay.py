#!/usr/bin/env python3
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Reproduce the manual, source-bound query identity encoding experiment."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess


HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]


def checked_bytes(data, expected):
    if hashlib.sha256(data).hexdigest() != expected:
        raise ValueError("Source hash mismatch; use the measured candidate checkout")
    return data


def verify_vectors(path):
    def field(value):
        data = value.encode("utf-8")
        return len(data).to_bytes(8, "little") + data

    for line in path.read_text().splitlines():
        record = json.loads(line)
        language = record["language"].strip().translate(
            str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz")
        )
        for domain, value, key, prefix in [
            ("hawdb-query-digest", record["normalized_query"], "query_digest", "q"),
            ("hawdb-query-text", record["text"], "query_text_hash", "t"),
        ]:
            framed = field(domain) + b"\x01\x00" + field(language) + field(value)
            expected = prefix + "1:" + hashlib.sha256(framed).hexdigest()
            if record[key] != expected:
                raise ValueError(f"Protocol mismatch: {record['case']} {key}")


def replay_host(binaries, output):
    signatures = {}
    common = {
        "backend": "memory",
        "path": "unused",
        "insert_single": "CREATE (:Boundary {id: $id, score: $score, title: $title})",
        "insert_bulk": "UNWIND $rows AS row CREATE (:Boundary {id: row.id, score: row.score, title: row.title})",
        "point": "MATCH (n:Boundary) WHERE n.id = $id RETURN n.id AS id, n.score AS score, n.title AS title",
        "scan": "MATCH (n:Boundary) RETURN n.id AS id, n.score AS score, n.title AS title",
        "columns": ["id", "score", "title"],
    }
    for case, count in [("point", 128), ("select", 4096), ("fill_bulk", 512)]:
        job = dict(common, case=case, rows=[
            {"id": i, "score": i * 0.5, "title": f"Synthetic row {i}"}
            for i in range(count)
        ])
        (output / f"{case}.json").write_text(json.dumps(job))
    with (output / "paired.jsonl").open("w") as stream:
        for sample in range(7):
            for case in ["point", "select", "fill_bulk"]:
                for mode in ["timing", "allocations"]:
                    labels = ["baseline", "candidate"]
                    if sample % 2:
                        labels.reverse()
                    for label in labels:
                        result = subprocess.run(
                            [str(binaries / f"{label}-{mode}"), str(output / f"{case}.json")],
                            cwd=REPO, check=True, capture_output=True, text=True,
                        )
                        data = json.loads(result.stdout)
                        if data["status"] != "ok":
                            raise ValueError(data)
                        signature = {key: data[key] for key in [
                            "backend", "case", "checksum", "durability", "input_rows",
                            "output_rows", "values", "engine_payload_bytes", "prefetch",
                        ]}
                        if signatures.setdefault(case, signature) != signature:
                            raise ValueError(f"Host result mismatch: {case}")
                        data.update(label=label, sample=sample, mode=mode)
                        stream.write(json.dumps(data) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--host-binaries", type=Path)
    args = parser.parse_args()
    manifest = json.loads((HERE / "manifest.json").read_text())
    for path, expected in manifest["candidate_source_sha256"].items():
        checked_bytes((REPO / path).read_bytes(), expected)
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    probe = output / "probe"
    (probe / "src").mkdir(parents=True)
    (probe / "baseline/src").mkdir(parents=True)
    baseline = subprocess.check_output([
        "git", "show", f"{manifest['baseline_revision']}:crates/query/src/lib.rs"
    ], cwd=REPO)
    (probe / "baseline/src/lib.rs").write_bytes(
        checked_bytes(baseline, manifest["baseline_query_source_sha256"])
    )
    (probe / "src/main.rs").write_bytes(checked_bytes(
        (HERE / "probe.rs").read_bytes(), manifest["probe_source_sha256"]
    ))
    shutil.copyfile(REPO / "bindings/benchmarks/native_alloc.rs", probe / "src/native_alloc.rs")
    (probe / "Cargo.lock").write_bytes(checked_bytes(
        (HERE / "probe.lock").read_bytes(), manifest["probe_lock_sha256"]
    ))
    (probe / "baseline/Cargo.toml").write_text('''[package]
name = "hawdb-query-baseline"
version = "0.0.0"
edition = "2024"
publish = false
license = "Apache-2.0"
[dependencies]
sha2 = { version = "=0.11.1", default-features = false }
''')
    candidate = json.dumps(str(REPO / "crates/query"))
    (probe / "Cargo.toml").write_text('''[package]
name = "query-identity-encoding-probe"
version = "0.0.0"
edition = "2024"
publish = false
license = "Apache-2.0"
[workspace]
members = ["baseline"]
[features]
allocation-probe = []
[dependencies]
baseline = { package = "hawdb-query-baseline", path = "baseline" }
serde_json = "=1.0.154"
''' + f'candidate = {{ package = "hawdb-query", path = {candidate} }}\n')
    env = dict(os.environ, CARGO_BUILD_JOBS="2")
    cargo = ["cargo", "run", "--locked", "--release", "--manifest-path", str(probe / "Cargo.toml")]
    for name, flags, arguments in [
        ("vectors.jsonl", [], ["--vectors"]),
        ("component-timing.jsonl", [], ["10000"]),
        ("component-allocations.jsonl", ["--features", "allocation-probe"], ["1000"]),
    ]:
        with (output / name).open("w") as stream:
            subprocess.run(cargo + flags + ["--"] + arguments, cwd=REPO,
                           env=env, stdout=stream, check=True)
    verify_vectors(output / "vectors.jsonl")
    if args.host_binaries:
        replay_host(args.host_binaries.resolve(), output)
    print(f"Replay artifacts: {output}")


if __name__ == "__main__":
    main()
