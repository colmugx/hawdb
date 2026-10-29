#!/usr/bin/env python3
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Prepare reproducible native internal-key probes without editing the registry.

Variants are upstream, fixed-head, compact separator, and their combination.
Each has an uninstrumented timing copy and a separately instrumented diagnostic
copy. All retain the repository toolchain/profile and exact dependency lock.
"""

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
import tomllib


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def replace(path, old, new, count=1):
    data = path.read_text()
    assert data.count(old) == count, (path, old, data.count(old))
    # The registry uses CRLF. Preserve it in modified upstream source files.
    path.write_bytes(data.replace(old, new).replace("\n", "\r\n").encode())


def diagnostic_harness(path):
    replace(path, 'fn diagnostic(_phase: &str) {}',
        'fn diagnostic(phase: &str) { eprintln!("{}", serde_json::json!({"phase": phase, "probe": bf_tree::structural_probe::snapshot()})); }')
    replace(path, '            for batch_first in [repetition % 2 == 0, repetition % 2 != 0] {',
        '            for batch_first in [repetition % 2 == 0, repetition % 2 != 0] {\n'
        '                let counters_before = bf_tree::structural_probe::snapshot()["counters"].clone();')
    replace(path, '                    scalar.push(nanos(began));\n'
        '                    assert_eq!(result, expected);\n                }\n            }',
        '                    scalar.push(nanos(began));\n'
        '                    assert_eq!(result, expected);\n                }\n'
        '                eprintln!("{}", serde_json::json!({"batch_probe": {"shape": batch_shape,\n'
        '                    "width": width, "epoch": snapshot.epoch(), "batch": batch_first,\n'
        '                    "before": counters_before, "after": bf_tree::structural_probe::snapshot()["counters"]}}));\n'
        '            }')


def patch(dependency, project, variant, diagnostic):
    nodes = dependency / "src/nodes"
    inner = nodes / "inner_node.rs"
    probe = (project / "structural/probe.rs").read_text().replace(
        "const ENABLED: bool = false", f"const ENABLED: bool = {str(diagnostic).lower()}")
    (dependency / "src/structural_probe.rs").write_text(probe)
    replace(dependency / "src/lib.rs", "mod nodes;", "mod nodes;\npub mod structural_probe;")
    replace(inner, "    pub(crate) fn free_node(ptr: *mut InnerNode) {",
            "    pub(crate) fn free_node(ptr: *mut InnerNode) {\n"
            "        crate::structural_probe::forget(ptr as usize);")
    replace(inner, "        self.disk_offset = disk_offset;\n\n        let offset =",
            "        crate::structural_probe::forget(self as *const Self as usize);\n"
            "        self.disk_offset = disk_offset;\n\n        let offset =")
    refresh = """
    fn probe_refresh(&self) {
        if crate::structural_probe::ENABLED {
            let keys = (1..self.meta.meta_count_with_fence())
                .map(|i| self.get_full_key(self.get_kv_meta(i))).collect();
            crate::structural_probe::refresh(self as *const Self as usize, keys);
        }
    }
"""
    replace(inner, "impl InnerNode {", "impl InnerNode {\n" + refresh)
    replace(inner, "        self.meta.increment_value_count();\n        true",
            "        self.meta.increment_value_count();\n        self.probe_refresh();\n        true")
    replace(inner, "            (&mut *ptr).version_lock = AtomicU64::new(0);",
            "            (&mut *ptr).version_lock = AtomicU64::new(0);\n"
            "            (&*ptr).probe_refresh();")
    replace(inner, "    pub(crate) fn lower_bound(&self, key: &[u8]) -> u64 {",
            "    pub(crate) fn lower_bound(&self, key: &[u8]) -> u64 {\n"
            "        crate::structural_probe::count(0, 1);")
    replace(inner, "        let mut cmp = prefix_key.cmp(search_key_prefix);",
            "        let mut cmp = prefix_key.cmp(search_key_prefix);\n"
            "        crate::structural_probe::count(1, 1);\n"
            "        crate::structural_probe::count(2, usize::from(cmp == Ordering::Equal));", 2)
    replace(inner, "        cmp = rest_key.cmp(search_key_postfix);",
            "        crate::structural_probe::count(3, 1);\n"
            "        crate::structural_probe::count(4, rest_key.len().min(search_key_postfix.len()));\n"
            "        cmp = rest_key.cmp(search_key_postfix);", 2)
    if variant != "upstream":
        replace(nodes / "mod.rs", "mod inner_node;", "mod inner_node;\nmod structural_keys;")
        kernel = (project / "structural/keys.rs").read_text()
        if variant == "head":
            kernel = kernel[:kernel.index("/// An inclusive right fence")]
        elif variant == "compact":
            start = kernel.index("/// An inclusive right fence")
            kernel = "// Copyright 2026 Nowledge\n// SPDX-License-Identifier: Apache-2.0\n\n" + kernel[start:kernel.index("#[cfg(test)]")]
        (nodes / "structural_keys.rs").write_text(kernel)
    if variant in ("head", "combined"):
        replace(inner, "prefix_key.cmp(search_key_prefix)",
                "super::structural_keys::compare_head(prefix_key, search_key_prefix)", 2)
        shortcut = """
        // Sorted pivot keys with equal first/last four bytes have the same
        // four-byte preview throughout. Short keys and small nodes fall back.
        let homogeneous = if upper >= 9 && key.len() >= 4 {
            let first = self.get_kv_meta(1);
            let last = self.get_kv_meta(upper - 1);
            first.key_len >= 4 && last.key_len >= 4
                && first.key_prefix == last.key_prefix
                && first.key_prefix.as_slice() == search_key_prefix
        } else { false };
        if homogeneous { crate::structural_probe::count(5, 1); }
"""
        replace(inner, "        while lower < upper {", shortcut + "\n        while lower < upper {")
        old = """            let mut cmp = super::structural_keys::compare_head(prefix_key, search_key_prefix);
        crate::structural_probe::count(1, 1);
        crate::structural_probe::count(2, usize::from(cmp == Ordering::Equal));"""
        new = """            let mut cmp = if homogeneous { Ordering::Equal } else {
                let result = super::structural_keys::compare_head(prefix_key, search_key_prefix);
                crate::structural_probe::count(1, 1);
                crate::structural_probe::count(2, usize::from(result == Ordering::Equal));
                result
            };"""
        replace(inner, old, new)
    if variant in ("compact", "combined"):
        # Change the fence BEFORE split_with_key installs it on both leaves
        # and the parent. Root's separate split implementation stays upstream.
        old = "        splitting_key.clone()\n    }"
        new = """        let predecessor = self.meta_iter().map(|m| self.get_full_key(m))
            .chain(mini_page.meta_iter().map(|m| mini_page.get_full_key(m)))
            .filter(|key| key.as_slice() < splitting_key.as_slice()).max();
        let candidate = predecessor.as_deref().and_then(|left|
            super::structural_keys::separator(left, splitting_key));
        if let Some(candidate) = candidate {
            let low = self.get_kv_meta(LOW_FENCE_IDX);
            if low.is_infinite_low_fence_key() || candidate > self.get_low_fence_key() {
                return candidate;
            }
        }
        splitting_key.clone()
    }"""
        replace(nodes / "leaf_node.rs", old, new)
    return sha(inner)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scratch", type=Path, required=True)
    args = parser.parse_args()
    project = Path(__file__).resolve().parents[1]
    repo = project.parents[1]
    scratch = args.scratch.resolve()
    scratch.mkdir(parents=True, exist_ok=False)
    metadata = json.loads(subprocess.check_output([
        "cargo", "metadata", "--locked", "--offline", "--format-version", "1",
        "--manifest-path", str(project / "Cargo.toml")], cwd=repo, text=True))
    package = next(p for p in metadata["packages"] if p["name"] == "bf-tree")
    registry = Path(package["manifest_path"]).parent
    lock = tomllib.loads((project / "Cargo.lock").read_text())
    locked = next(p for p in lock["package"] if p["name"] == "bf-tree")
    archive = registry.parents[2] / "cache" / registry.parent.name / "bf-tree-0.5.6.crate"
    assert package["version"] == "0.5.6" and sha(archive) == locked["checksum"]
    vcs = json.loads((registry / ".cargo_vcs_info.json").read_text())
    assert vcs["git"]["sha1"] == "3998ce8643dec4e98b07f2d11cad67bf2c2d28d1"
    (scratch / ".cargo").mkdir()
    for source in (repo / ".cargo/config.toml", repo / "rust-toolchain.toml"):
        destination = scratch / (".cargo/config.toml" if source.name == "config.toml" else source.name)
        shutil.copy2(source, destination)
    local_sources = {str(p.relative_to(repo)): sha(p)
        for crate in ("qos", "storage") for p in (repo / "crates" / crate).rglob("*")
        if p.is_file() and (p.suffix == ".rs" or p.name == "Cargo.toml")}
    receipt = {"upstream_commit": vcs["git"]["sha1"], "registry_checksum": locked["checksum"],
        "repository_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip(),
        "profile_sha256": sha(repo / ".cargo/config.toml"),
        "toolchain_sha256": sha(repo / "rust-toolchain.toml"),
        "local_dependency_sources": local_sources, "variants": {}}
    for diagnostic in (False, True):
        for variant in ("upstream", "head", "compact", "combined"):
            name = variant + ("-diagnostic" if diagnostic else "")
            root = scratch / name
            root.mkdir()
            with tarfile.open(archive) as packed:
                packed.extractall(root, filter="data")
            dependency = root / "bf-tree"
            (root / "bf-tree-0.5.6").rename(dependency)
            patch(dependency, project, variant, diagnostic)
            harness = root / "harness"
            harness.mkdir()
            for folder in ("src", "simd", "structural"):
                shutil.copytree(project / folder, harness / folder)
            if diagnostic:
                diagnostic_harness(harness / "src/performance.rs")
            manifest = (project / "Cargo.toml").read_text()
            for crate in ("qos", "storage"):
                manifest = manifest.replace(f'"../../crates/{crate}"', json.dumps(str(repo / "crates" / crate)))
            manifest += f'\n[patch.crates-io]\nbf-tree = {{ path = {json.dumps(str(dependency))} }}\n'
            (harness / "Cargo.toml").write_text(manifest)
            shutil.copy2(project / "Cargo.lock", harness / "Cargo.lock")
            subprocess.run(["cargo", "metadata", "--offline", "--format-version", "1", "--manifest-path", str(harness / "Cargo.toml")], cwd=scratch, stdout=subprocess.DEVNULL, check=True)
            expected = json.loads(json.dumps(lock))
            entry = next(p for p in expected["package"] if p["name"] == "bf-tree")
            del entry["source"]
            del entry["checksum"]
            assert tomllib.loads((harness / "Cargo.lock").read_text()) == expected
            receipt["variants"][name] = {"manifest": str(harness / "Cargo.toml"),
                "diagnostic": diagnostic, "source_sha256": {
                    str(p.relative_to(root)): sha(p) for p in root.rglob("*")
                    if p.is_file() and (p.suffix == ".rs" or p.name in ("Cargo.toml", "Cargo.lock"))}}
    (scratch / "source.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(scratch / "source.json")


if __name__ == "__main__":
    main()
