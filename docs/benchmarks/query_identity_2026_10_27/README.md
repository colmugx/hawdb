# Request-owned query identity allocation probe

Baseline: `28b8fdb3026234516f1e18063252d7d70a9706ca`.
Candidate library sources and binaries are SHA-256-bound in [manifest.json](manifest.json).
All builds use Rust 1.97.1, the repository dev profile and default features on
`aarch64-apple-darwin`, with two Cargo build jobs. No compilation profile changes.

The change moves the planner's current-request identity to the ordinary or
concurrent statement recorder. It does not change normalization or either
versioned hash protocol, remove parsing, or introduce a cross-request text cache.
The existing fallback still constructs identities when planning fails or the
caller has no identity. SQL, EXPLAIN and deferred retained cursors are unchanged.
The three strings live until observation completes instead of being dropped
after planning; no identity is stored in a shared plan-cache entry.

## Facade allocator observations

[query_identity_probe.rs](query_identity_probe.rs) uses the real ordinary and
concurrent APIs, an equality index, 64 synthetic nodes, 32 warmup calls and
256 measured point/update calls or 16 full scans. Three alternating process
pairs per facade/case produce the 36 records in [probe-paired.jsonl](probe-paired.jsonl).
Each result validates row counts; point/update results also validate exact values.

Median Rust allocator requests per call, including successful reallocations:

| Facade / case | Baseline | Candidate | Requested byte reduction per call |
| --- | ---: | ---: | ---: |
| Ordinary / point | 510 | 493 | 2,546 |
| Concurrent / point | 523 | 506 | 2,546 |
| Ordinary / scan | 810 | 794 | 2,203 |
| Concurrent / scan | 823 | 807 | 2,203 |
| Ordinary / update | 1,293.035 | 1,276.035 | 2,630 |
| Concurrent / update | 1,207 | 1,190 | 2,630 |

Each affected call removes one identity construction: 16 or 17 allocator
requests, including 11 successful resizes. Scan/point samples return to their
starting live requested-byte counts. Update samples retain identical live
deltas before/after the change: 31,008 bytes for ordinary and 32,992 bytes for
concurrent, across each complete 256-update sample. This is allocator-request
accounting, not an RSS, peak-residency or physical-copy measurement.

## Existing admitted-host controls

The unchanged `host_boundary` and `host_boundary_allocations` benches provide
84 alternating process samples in [paired.jsonl](paired.jsonl): seven pairs
for each of point (128 nodes/calls), select (4,096 rows) and bulk insert
(512 rows followed by a complete scan), with instrumented allocation and
uninstrumented timing collected separately. Every result retains the same
checksum, output count and schema across revisions.

Admitted point/select reads execute on a read transaction without a statement
recorder, so they have no duplicate identity construction to remove. Their
allocation counts and requested bytes are exactly unchanged. Bulk insert uses
ordinary mutation observation and saves 17 requests / 2,626 requested bytes.
Uninstrumented query-boundary medians in nanoseconds were:

| Case | Baseline | Candidate |
| --- | ---: | ---: |
| Point | 6,359,448 | 6,210,460 |
| Select | 5,463,583 | 5,204,834 |
| Bulk insert and scan | 4,740,083 | 4,671,791 |

Individual timing samples overlap and vary substantially. These short dev-profile
observations do not establish a latency improvement, p99 behavior, release-mode
throughput or remote CI speedup. The baseline's first build took 173 seconds;
the existing-bench candidate rebuild took 10.66 seconds. Compilation is outside
all measurement intervals.

## Reproduction

Use separate baseline/candidate checkouts and the same pinned toolchain. Copy
`query_identity_probe.rs` temporarily to each checkout's `examples/` directory,
then build from that checkout's root:

```sh
CARGO_BUILD_JOBS=2 cargo build --locked --example query_identity_probe \
  --bench host_boundary --bench host_boundary_allocations
```

Copy the example executable and the two bench executables into one scratch
directory, using `baseline-probe`, `baseline-timing`, `baseline-allocations`
and the corresponding `candidate-*` names. Bench executables are in
`target/debug/deps/` with Cargo's hash suffix. Run:

```sh
python3 docs/benchmarks/query_identity_2026_10_27/replay.py /path/to/scratch
```

The script regenerates synthetic jobs, runs process pairs serially in alternating
order and writes raw JSONL. These are manual probes; no benchmark, fuzz campaign
or new job is registered in CI.
