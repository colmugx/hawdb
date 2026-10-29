#!/usr/bin/env python3
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Automatic-checkpoint selections and complete runtime configuration coverage."""

import argparse
from collections import Counter
import itertools
import json
from pathlib import Path
import re
import sys

AUTOMATIC_PREFIX = "api::tests::power_loss::automatic_checkpoint::"
AUTOMATIC_CASES = {
    "head-every-write": "automatic_head_cuts_preserve_synchronous_transactions_across_three_generations",
    "head-checkpoint": "automatic_head_cuts_preserve_relaxed_transactions_across_three_generations",
    "manifest-every-write": "automatic_manifest_cuts_preserve_synchronous_transactions_across_three_generations",
    "manifest-checkpoint": "automatic_manifest_cuts_preserve_relaxed_transactions_across_three_generations",
    "root-every-write": "automatic_root_cuts_preserve_synchronous_transactions_across_three_generations",
    "root-checkpoint": "automatic_root_cuts_preserve_relaxed_transactions_across_three_generations",
    "wal-sync-every-write": "automatic_wal_sync_cuts_preserve_synchronous_transactions_across_three_generations",
    "wal-sync-checkpoint": "automatic_wal_sync_cuts_preserve_relaxed_transactions_across_three_generations",
}
CUTS = {
    "head-every-write": "Head",
    "head-checkpoint": "Head",
    "root-every-write": "SealedRoot",
    "root-checkpoint": "SealedRoot",
    "wal-sync-every-write": "CandidateWalSync",
    "wal-sync-checkpoint": "CandidateWalSync",
}
POLICIES = ("SyncOnEveryWrite", "SyncOnCheckpoint")


def expected_combinations(shard):
    if shard == "remaining":
        return set()
    if shard == "full":
        return set().union(*(expected_combinations(key) for key in AUTOMATIC_CASES))
    if shard not in AUTOMATIC_CASES:
        raise ValueError(f"unknown automatic checkpoint shard: {shard}")
    cut = CUTS.get(shard, "Manifest")
    policies = POLICIES
    if shard.endswith("-every-write"):
        policies = POLICIES[:1]
    elif shard.endswith("-checkpoint"):
        policies = POLICIES[1:]
    return set(itertools.product([cut], policies, ["Before", "After"], range(1, 4)))


def expected_summaries(shard):
    if shard == "remaining":
        return Counter()
    if shard == "full":
        return sum((expected_summaries(key) for key in AUTOMATIC_CASES), Counter())
    configurations = expected_combinations(shard)
    cut = next(iter(configurations))[0]
    policies = {key[1] for key in configurations}
    return Counter({(cut, len(policies)): 1})


def validate_log(shard, text):
    expected = Counter({key: 1 for key in expected_combinations(shard)})
    actual = Counter()
    for line in text.splitlines():
        if "automatic-power-v4 " not in line:
            continue
        fields = dict(re.findall(r"([a-z_]+)=(\S+)", line))
        try:
            key = (
                fields["cut"],
                fields["policy"],
                fields["boundary"],
                int(fields["generation"]),
            )
        except (KeyError, ValueError) as error:
            raise ValueError(
                "incomplete automatic checkpoint coverage record"
            ) from error
        actual[key] += 1
    if actual != expected:
        raise ValueError(
            f"automatic coverage differs: missing={expected - actual}, "
            f"unexpected={actual - expected}"
        )
    summaries = Counter()
    images = 0
    pattern = (
        r"automatic (\w+) qualification: (\d+) physical crash images, "
        r"3 generations, (\d+) durability modes, both boundaries"
    )
    for cut, count, policies in re.findall(pattern, text):
        count = int(count)
        if count <= 0:
            raise ValueError(
                "automatic checkpoint selection evaluated no physical images"
            )
        summaries[(cut, int(policies))] += 1
        images += count
    if summaries != expected_summaries(shard):
        raise ValueError("automatic checkpoint completion summaries differ")
    return {
        "protocol": "hawdb-automatic-checkpoint-coverage-v1",
        "shard": shard,
        "configurations": sum(actual.values()),
        "physical_images": images,
    }


def main():
    sys.stdout.reconfigure(newline="\n")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("shard", choices=[*AUTOMATIC_CASES, "full", "remaining"])
    parser.add_argument("--validate-log", type=Path, required=True)
    args = parser.parse_args()
    print(
        json.dumps(
            validate_log(args.shard, args.validate_log.read_text(encoding="utf-8"))
        )
    )


if __name__ == "__main__":
    main()
