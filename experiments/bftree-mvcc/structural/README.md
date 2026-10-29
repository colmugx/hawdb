# Internal keys, batches and retained-version collection

Manual, isolated probes for [#1237](https://github.com/nowledge-co/hawdb/issues/1237).
The default adapter still uses the unmodified `bf-tree = 0.5.6` registry
dependency. No probe is registered in CI or used by a production library.
See the [source-bound measurements and limitations](../measurements/structural-20261024/README.md)
for the final cohorts and the decision to retain the default registry dependency.

## PostgreSQL precedents and adaptation boundaries

PostgreSQL reduces upper-tree space through suffix truncation when splitting
leaves, improving internal-page fanout. Its implementation truncates trailing
**attributes**, with explicit ordering semantics for missing attributes; this
does not directly implement a minimal byte separator for Bf-Tree. Its split
selection also considers free space, duplicate groups and truncation potential.
See the [upstream README](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/access/nbtree/README)
and [split selection](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/access/nbtree/nbtsplitloc.c).

[PostgreSQL 17](https://www.postgresql.org/docs/17/release-17.html) improved
multi-value `IN` searches. Its B-tree scans also copy matching page items before
releasing the page lock. These are useful precedents for batching and position
reuse; they do not establish that Bf-Tree's public iterator can reuse every
native root descent across page boundaries.

[Bottom-up deletion](https://www.postgresql.org/docs/18/btree.html#BTREE-DELETION)
targets version churn before a split. The adapter instead owns explicit
epoch-encoded history and must preserve all active read/write snapshots and a
visible anchor. PostgreSQL posting lists deduplicate equal index keys without
discarding their TIDs; epoch-specific values/tombstones cannot simply be merged
or dropped on the same basis. PostgreSQL 18's
[skip scan](https://www.postgresql.org/docs/18/indexes-multicolumn.html) is a
separate composite-index navigation optimization, not Bloom filtering.

## Native working probes

`prepare.py` verifies the registry archive checksum, upstream commit and exact
dependency lock. It extracts temporary copies, preserves Microsoft's MIT
headers, applies reviewable source substitutions, and copies the adapter plus
repository toolchain/profile. It never modifies the registry or default
manifest. The four variants are:

| Variant | Change |
| --- | --- |
| `upstream` | Original ordering/fences; disabled diagnostic hooks |
| `head` | Big-endian four-byte comparisons; skip ineffective previews within sufficiently populated nodes whose sorted endpoint previews equal the search preview |
| `compact` | Choose a shorter byte separator between the actual maximum left key and the selected minimum right key before installing both leaf fences and the parent pivot |
| `combined` | Both candidates |

The head shortcut requires at least eight real pivots and full four-byte
endpoint keys. Sortedness makes all intermediate previews equal. Short keys,
nonmatching previews and smaller nodes follow the original comparisons. Full
postfix ordering stays with Rust's existing byte comparator.

The separator satisfies `left_max < separator <= right_min`, retains at least
four bytes when possible and must remain above the original low fence. It is
chosen in the native merge/split path before either leaf's fence is installed;
truncating only the parent key would misroute gap insertions. The separate
root-split path stays upstream. Leaf-prefix compression can lose effectiveness
with shorter fences, and split preparation performs extra work, so space
savings do not imply an overall latency improvement.

Each variant has a separate `-diagnostic` copy. It records:

- Actual pivot key lengths, stored postfix bytes, endpoint common prefixes and
  distinct four-byte previews for live inner nodes.
- Lower-bound calls, actual preview comparisons/equalities, postfix comparison
  calls and homogeneous-preview shortcuts.

Postfix operand span is an upper bound on bytes considered, not a measurement
of actual machine comparisons. The owned node summaries are refreshed while
existing mutation paths already protect the node and forgotten before free;
the reporting thread never follows raw native pointers. Counts include all
live generations and native worker threads, not just the current generation.
**Diagnostic timings must never be used as performance evidence.**

## Reproduction

Use Python 3.12 or newer. Run from the repository root. All scratch, output and
journal directories below must be new.

```console
python3.12 experiments/bftree-mvcc/structural/prepare.py --scratch /tmp/bftree-structural
python3.12 experiments/bftree-mvcc/structural/build.py --scratch /tmp/bftree-structural --target-dir /tmp/bftree-release --dev-target-dir /tmp/bftree-dev
python3.12 experiments/bftree-mvcc/structural/measure.py --scratch /tmp/bftree-structural --output /tmp/bftree-native-results --journals /tmp/bftree-native-journals --mode native
python3.12 experiments/bftree-mvcc/structural/measure.py --scratch /tmp/bftree-structural --output /tmp/bftree-maintenance-results --journals /tmp/bftree-maintenance-journals --mode maintenance
python3.12 experiments/bftree-mvcc/structural/measure.py --scratch /tmp/bftree-structural --output /tmp/bftree-diagnostic-results --journals /tmp/bftree-diagnostic-journals --mode diagnostic
```

An optional `--mode baseline --baseline /path/to/8b0744932-binary` compares the
unchanged shaped benchmark against the preceding adapter. `audit` only
normalizes old/new synthetic transaction evidence; it provides no compatibility
opener or migration for development databases. The driver requires actual byte
equality of normalized epochs, operation IDs and full mutation payloads across
each matched group. Every advanced benchmark also reopens and verifies current
data through ordinary strict recovery. Maintenance records legitimately make
raw journal hashes different.

Use `--default-binary /path/to/frozen-default-build` for baseline and maintenance
comparisons against the actual unmodified registry build; it also supplies the
auditor. Without it, these modes use the uninstrumented temporary upstream copy.
Diagnostic harnesses additionally record native comparison counters around
each scalar/batched request outside its timing interval; these process-wide
counts can include concurrent native workers.

Five matched repetitions rotate executable/layout order across short keys at
16/2, 512/24 and 2,048/32, plus common-prefix/spread keys at 512/24, following
the earlier SIMD matrix's representative shape selection. The diagnostic mode
also covers both long shapes at 2,048/32; its timings do not qualify those
larger long-key cases. `--matrix full` optionally expands timing to all nine
shape/size combinations. The 2,048-key advanced case uses 32 rounds so its protected full checkpoint
fits the unchanged explicit byte bounds. Raw results retain every adverse
sample, compiler counts, load, binary identity, full reports and diagnostics.
Source/build receipts include first-build elapsed cost; no private optimization
profiles or target CPU flags are permitted. `cow` is a component control and
shared-host measurements do not establish remote CI or production speedups.
