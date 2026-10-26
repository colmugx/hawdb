#!/usr/bin/env python3
"""Stage an auditable, OS/architecture-specific *review* archive.

This deliberately does not publish to mooncakes or GitHub Releases: upstream
needs to approve the final package model, dependency notices and ABI policy.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import subprocess
import tarfile

MODULE = pathlib.Path(__file__).resolve().parents[1]
REPOSITORY = MODULE.parent.parent


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--os", required=True)
    parser.add_argument("--arch", required=True)
    args = parser.parse_args()
    manifest = [
        ("moon.mod", MODULE / "moon.mod"),
        ("moon.pkg", MODULE / "moon.pkg"),
        ("hawdb.mbt", MODULE / "hawdb.mbt"),
        ("value.mbt", MODULE / "value.mbt"),
        ("transaction.mbt", MODULE / "transaction.mbt"),
        ("traits.mbt", MODULE / "traits.mbt"),
        ("README.md", MODULE / "README.md"),
        ("BUILDING.md", MODULE / "BUILDING.md"),
        ("tools/hawdb_derive.py", MODULE / "tools/hawdb_derive.py"),
        ("tools/README.md", MODULE / "tools/README.md"),
        ("LICENSE", REPOSITORY / "LICENSE"),
        (
            "native/target/release/libhawdb_moonbit.a",
            MODULE / "native" / "target" / "release" / "libhawdb_moonbit.a",
        ),
    ]
    for _, path in manifest:
        if not path.is_file():
            raise SystemExit(f"Missing required release staging input: {path}")
    import re
    mod = (MODULE / "moon.mod").read_text(encoding="utf-8")
    match = re.search(r'(?m)^version\s*=\s*"([^"]+)"', mod)
    if match is None:
        raise SystemExit("moon.mod must have a package version")
    slug = f"hawdb-moonbit-{match.group(1)}-{args.os.lower()}-{args.arch.lower()}"
    dist = MODULE / "dist"
    dist.mkdir(exist_ok=True)
    output = dist / f"{slug}.tar.gz"
    provenance = {
        "kind": "review-staging-not-published",
        "commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=REPOSITORY, text=True
        ).strip(),
        "os": args.os,
        "arch": args.arch,
        "rustc": subprocess.check_output(["rustc", "--version"], text=True).strip(),
        "moon": subprocess.check_output(["moon", "version", "--all"], text=True).strip(),
        "native_library_sha256": hashlib.sha256(manifest[-1][1].read_bytes()).hexdigest(),
    }
    metadata = dist / "PROVENANCE.json"
    metadata.write_text(json.dumps(provenance, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    with tarfile.open(output, "w:gz") as archive:
        for inside, path in sorted(manifest):
            archive.add(path, arcname=f"{slug}/{inside}", recursive=False)
        archive.add(metadata, arcname=f"{slug}/PROVENANCE.json")
    checksum = hashlib.sha256(output.read_bytes()).hexdigest()
    (dist / f"{slug}.tar.gz.sha256").write_text(
        f"{checksum}  {output.name}\n", encoding="ascii"
    )
    print(f"STAGED {output.name}")
    print(f"SHA256 {checksum}")


if __name__ == "__main__":
    main()
