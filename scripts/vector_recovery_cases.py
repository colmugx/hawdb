#!/usr/bin/env python3
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Shared native recovery selections and complete runtime matrix validation."""

import argparse
from collections import Counter
import itertools
import json
from pathlib import Path
import re
import sys

RECOVERY_PREFIX = "api::tests::power_loss::consumer_vector_incremental::faults::recovery::"
CASE_PREFIX = RECOVERY_PREFIX + "vector_index_incremental_power_loss_recovery_publication_"
RECOVERY_CASES = {
    "recovery-births": CASE_PREFIX + "retains_unselected_births",
    "recovery-unselected": CASE_PREFIX + "settles_unselected_intents",
    "recovery-selected": CASE_PREFIX + "completes_selected_intents",
    "recovery-lost-sql": CASE_PREFIX + "preserves_lost_relaxed_sql_sources",
    "recovery-lost-nodes": CASE_PREFIX + "preserves_lost_relaxed_node_sources",
}
CUTS = ("Write", "FileSync(Before)", "FileSync(After)", "Rename(Before)",
        "Rename(After)", "Directory(Before)", "Directory(After)")


def expected_combinations(shard):
    if shard == "full":
        return set().union(*(expected_combinations(key) for key in RECOVERY_CASES))
    if shard == "remaining":
        return set()
    origins = {
        "recovery-births": "Link",
        "recovery-unselected": "SelectorRename(Before)",
        "recovery-selected": "SelectorRename(After)",
    }
    if shard in origins:
        return set(itertools.product(
            [origins[shard]], CUTS,
            ["SyncOnEveryWrite", "SyncOnCheckpoint"], ["false", "true"], ["true"],
        ))
    if shard not in RECOVERY_CASES:
        raise ValueError(f"unknown vector recovery shard: {shard}")
    graph = "true" if shard == "recovery-lost-nodes" else "false"
    return set(itertools.product(
        ["Link", "SelectorWrite"], CUTS, ["SyncOnCheckpoint"], [graph], ["false"],
    ))


def validate_log(shard, text):
    expected = Counter({key: 1 for key in expected_combinations(shard)})
    actual = Counter()
    plans = reopens = 0
    for line in text.splitlines():
        if "canonical-vector-recovery-power-v1 " not in line:
            continue
        fields = dict(re.findall(r"([a-z_]+)=(\S+)", line))
        try:
            key = tuple(fields[name] for name in (
                "origin", "cut", "durability", "graph", "source_updated"
            ))
            count, opened = int(fields["plans"]), int(fields["distinct_reopens"])
        except (KeyError, ValueError) as error:
            raise ValueError("incomplete recovery coverage record") from error
        if count <= 0 or opened <= 0:
            raise ValueError("recovery combination has no evaluated physical plans")
        actual[key] += 1
        plans += count
        reopens += opened
    if actual != expected:
        raise ValueError(f"recovery coverage differs: missing={expected - actual}, "
                         f"unexpected={actual - expected}")
    return {"protocol": "hawdb-vector-recovery-coverage-v1", "shard": shard,
            "combinations": sum(actual.values()), "fault_plans": plans,
            "distinct_reopens": reopens}


def main():
    # Git Bash strips LF from command substitution but would retain a CR.
    sys.stdout.reconfigure(newline="\n")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("shard", nargs="?", choices=[*RECOVERY_CASES, "full", "remaining"])
    parser.add_argument("--prefix", action="store_true")
    parser.add_argument("--validate-log", type=Path)
    args = parser.parse_args()
    if args.prefix and not args.shard and not args.validate_log:
        print(RECOVERY_PREFIX)
    elif args.shard and args.validate_log and not args.prefix:
        print(json.dumps(validate_log(args.shard, args.validate_log.read_text(encoding="utf-8"))))
    elif args.shard in RECOVERY_CASES and not args.prefix:
        print(RECOVERY_CASES[args.shard])
    else:
        parser.error("select a recovery shard, a coverage log, or --prefix")


if __name__ == "__main__":
    main()
