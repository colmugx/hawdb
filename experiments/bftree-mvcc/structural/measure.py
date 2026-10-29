#!/usr/bin/env python3
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Manual matched release comparisons and separate internal-node diagnostics.

Never compiles during measurement, changes profiles, discards adverse samples,
or treats observable cursor starts as an exact count of native root descents.
"""

import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import time


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def compilers():
    names = subprocess.check_output(["ps", "-axo", "comm="], text=True).splitlines()
    return sum(Path(name.strip()).name in ("rustc", "clippy-driver") for name in names)


def leaves(value, path=()):
    if isinstance(value, dict):
        for key, child in value.items():
            yield from leaves(child, path + (key,))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from leaves(child, path + (str(index),))
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        yield "/".join(path), value


def summarize(samples):
    result = []
    for identity, pairs in samples.items():
        names = list(next(iter(pairs.values())))
        reference = names[0]
        row = {"identity": identity, "reference": reference, "pairs": len(pairs), "metrics": {}}
        metrics = {name: {pair: dict(leaves(values[name])) for pair, values in pairs.items()} for name in names}
        fields = {field for values in metrics.values() for sample in values.values() for field in sample}
        for field in sorted(fields):
            details = {}
            for name in names:
                values = [metrics[name][pair].get(field) for pair in pairs]
                if any(value is None for value in values):
                    continue
                ratios = [metrics[name][pair][field] / metrics[reference][pair][field]
                    for pair in pairs if metrics[reference][pair].get(field, 0) > 0]
                details[name] = {"median": statistics.median(values), "values": values,
                    "paired_ratios": ratios,
                    "paired_ratio_median": statistics.median(ratios) if ratios else None}
            row["metrics"][field] = details
        result.append(row)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--journals", type=Path, required=True)
    parser.add_argument("--mode", choices=("native", "maintenance", "diagnostic", "baseline"), required=True)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--default-binary", type=Path,
                        help="frozen default-registry build for maintenance/baseline and audit")
    parser.add_argument("--pairs", type=int, default=5)
    parser.add_argument("--matrix", choices=("representative", "full"), default="representative")
    args = parser.parse_args()
    assert args.pairs >= 5 or args.mode == "diagnostic"
    args.output.mkdir(parents=True, exist_ok=False)
    args.journals.mkdir(parents=True, exist_ok=False)
    scratch = args.scratch.resolve()
    variants = ["upstream", "head", "compact", "combined"]
    binaries = {name: scratch / "bin" / name for name in variants}
    default_binary = args.default_binary.resolve() if args.default_binary else binaries["upstream"]
    if args.mode == "diagnostic":
        binaries = {name: scratch / "bin" / (name + "-diagnostic") for name in variants}
    if args.mode == "baseline":
        assert args.baseline is not None
        binaries = {"baseline": args.baseline.resolve(), "upstream": default_binary}
    if args.mode == "maintenance":
        binaries = {"keep": default_binary, "collect": default_binary}
    protocol = {
        "mode": args.mode, "pairs": 1 if args.mode == "diagnostic" else args.pairs,
        "driver_sha256": digest(Path(__file__)),
        "prepared_source_receipt_sha256": digest(scratch / "source.json"),
        "layouts": ["versioned", "hybrid", "cow"],
        "shapes": ["numeric8", "prefix64", "spread64"],
        "workloads": [[16, 2], [512, 24], [2048, 32]],
        "matrix": args.matrix,
        "cases": [[16, 2, "numeric8"], [512, 24, "numeric8"], [2048, 32, "numeric8"],
                  [512, 24, "prefix64"], [512, 24, "spread64"]]
                 if args.matrix == "representative" else
                 [[keys, rounds, shape] for keys, rounds in ((16, 2), (512, 24), (2048, 32))
                  for shape in ("numeric8", "prefix64", "spread64")],
        "profile": "uniform repository release; Rust 1.97.1; no custom compiler flags",
        "platform": platform.platform(),
        "order": "rotate layouts/variants, reverse shapes/workloads/variants on odd pairs",
        "durability": "sync_all per transaction and complete maintenance record",
        "transaction_gate": "strict framed read and exact normalized transaction bytes; maintenance validated by benchmark reopen",
        "binaries": {name: {"path": str(path), "sha256": digest(path)} for name, path in binaries.items()},
        "limits": "shared host; native Memory backend; no production/power-loss qualification; diagnostics never qualify timing",
        "batch_tail": "per request, 32 samples; p99 is the maximum",
        "load_observation": "before/after compiler counts and load do not establish continuous quiescence",
        "audit_binary_sha256": digest(default_binary),
    }
    if args.mode == "diagnostic":
        protocol["cases"] = [[2048, 32, shape] for shape in protocol["shapes"]]
        protocol["workloads"] = [[2048, 32]]
        protocol["layouts"] = ["versioned", "hybrid"]
    (args.output / "protocol.json").write_text(json.dumps(protocol, indent=2) + "\n")
    audit_binary = default_binary
    samples = {}
    with gzip.open(args.output / "results.jsonl.gz", "wt") as raw:
        for pair in range(protocol["pairs"]):
            workloads = protocol["workloads"][::-1 if pair % 2 else 1]
            if args.mode == "diagnostic":
                workloads = [[2048, 32]]
            layouts = protocol["layouts"]
            layouts = layouts[pair % 3:] + layouts[:pair % 3]
            names = list(binaries)
            execution_order = names[pair % len(names):] + names[:pair % len(names)]
            if pair % 2:
                execution_order.reverse()
            for keys, rounds in workloads:
                for shape in protocol["shapes"][::-1 if pair % 2 else 1]:
                    if args.mode != "diagnostic" and [keys, rounds, shape] not in protocol["cases"]:
                        continue
                    for layout in layouts:
                        if args.mode == "diagnostic" and layout == "cow":
                            continue
                        reports = {}
                        streams = []
                        for name in execution_order:
                            path = args.journals / f"{pair}-{keys}-{rounds}-{shape}-{layout}-{name}.journal"
                            advanced = args.mode in ("maintenance", "diagnostic")
                            command = [str(binaries[name]), "bench-advanced" if advanced else "bench-shaped",
                                str(path), layout, str(keys), str(rounds), shape]
                            if advanced:
                                command.append("collect" if name == "collect" else "keep")
                            began = time.monotonic_ns()
                            load = os.getloadavg()
                            compiling = compilers()
                            completed = subprocess.run(command, capture_output=True, text=True, timeout=300)
                            receipt = {"pair": pair, "variant": name, "layout": layout,
                                "keys": keys, "rounds": rounds, "shape": shape, "command": command,
                                "load_before": load, "load_after": os.getloadavg(),
                                "compilers_before": compiling, "compilers_after": compilers(),
                                "elapsed_ns": time.monotonic_ns() - began,
                                "returncode": completed.returncode, "stdout": completed.stdout, "stderr": completed.stderr,
                                "journal_bytes": path.stat().st_size if path.exists() else None}
                            raw.write(json.dumps(receipt) + "\n"); raw.flush()
                            assert completed.returncode == 0, receipt
                            report = json.loads(completed.stdout)
                            assert report["profile"] == "release"
                            audit = subprocess.run([str(audit_binary), "audit", str(path), layout], capture_output=True, timeout=60)
                            assert audit.returncode == 0, audit.stderr
                            # Retain only a digest, but require actual byte equality
                            # between all members of this matched group below.
                            streams.append(audit.stdout)
                            reports[name] = report
                            raw.write(json.dumps({"evidence": {
                                "pair": pair, "variant": name, "layout": layout,
                                "keys": keys, "rounds": rounds, "shape": shape,
                                "normalized_transactions_sha256": hashlib.sha256(audit.stdout).hexdigest(),
                                "journal_sha256": digest(path)}}) + "\n"); raw.flush()
                        assert all(stream == streams[0] for stream in streams)
                        identity = f"{keys}/{rounds}/{shape}/{layout}"
                        samples.setdefault(identity, {})[pair] = {name: reports[name] for name in names}
            (args.output / "summary.json").write_text(json.dumps(summarize(samples), indent=2) + "\n")
            print(args.mode, "completed", pair + 1, "/", protocol["pairs"], flush=True)


if __name__ == "__main__":
    main()
