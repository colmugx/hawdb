#!/usr/bin/env python3
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Manually check prepared internal-key variants with the uniform profiles."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--target-dir", type=Path, required=True)
    parser.add_argument("--dev-target-dir", type=Path, required=True)
    parser.add_argument("--jobs", type=int, default=2)
    args = parser.parse_args()
    scratch = args.scratch.resolve()
    sources = json.loads((scratch / "source.json").read_text())
    binaries = scratch / "bin"
    binaries.mkdir(exist_ok=False)
    logs = scratch / "validation"
    logs.mkdir(exist_ok=False)
    env = os.environ.copy()
    assert not any(value and (name in ("RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS")
        or name.startswith("CARGO_PROFILE_")
        or (name.startswith("CARGO_TARGET_") and name.endswith("RUSTFLAGS")))
        for name, value in env.items()), "compiler/profile override"
    env["CARGO_BUILD_JOBS"] = str(args.jobs)
    receipts = []
    for name, source in sources["variants"].items():
        root = scratch / name
        for relative, digest in source["source_sha256"].items():
            assert hashlib.sha256((root / relative).read_bytes()).hexdigest() == digest
        stages = [
            ("test-dev", ["test", "--all-targets", "--all-features"], args.dev_target_dir),
            ("clippy", ["clippy", "--all-targets", "--all-features", "--", "-D", "warnings"], args.dev_target_dir),
            ("test-release", ["test", "--release", "--all-targets", "--all-features"], args.target_dir),
            ("build-release", ["build", "--release"], args.target_dir),
        ]
        for stage, options, target in stages:
            command = ["cargo", options[0], "--locked", "--manifest-path", source["manifest"], *options[1:]]
            env["CARGO_TARGET_DIR"] = str(target.resolve())
            began = time.monotonic()
            print(name, stage, "starting", flush=True)
            with (logs / f"{name}-{stage}.log").open("w") as log:
                result = subprocess.run(command, cwd=scratch, env=env, stdout=log, stderr=subprocess.STDOUT)
            receipts.append({"variant": name, "stage": stage, "command": command,
                "returncode": result.returncode, "seconds": time.monotonic() - began})
            (logs / "build.json").write_text(json.dumps(receipts, indent=2) + "\n")
            print(name, stage, "exit", result.returncode, "seconds", round(receipts[-1]["seconds"], 2), flush=True)
            assert result.returncode == 0, logs / f"{name}-{stage}.log"
        binary = binaries / name
        shutil.copy2(args.target_dir.resolve() / "release/hawdb-bftree-mvcc-experiment", binary)
        (binaries / f"{name}.sha256").write_text(hashlib.sha256(binary.read_bytes()).hexdigest() + "\n")


if __name__ == "__main__":
    main()
