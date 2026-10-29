# Borrowed statement-summary completion

Baseline production revision: `d2511859f0952680eb93cd8393c747e9be72781f`.
Candidate source and binary SHA-256 values are in [manifest.json](manifest.json).
Rust 1.97.1, aarch64 macOS, default features and the repository dev profile are
used throughout, with two Cargo build jobs. No compilation profile changes.

Successful summary hits update the existing first sample's counters through a
borrowed digest. Metadata becomes owned on insertion. The existing owned API
also borrows the lookup key on hits. Failed facade executions keep their owned
construction, formatting/truncation and pre-telemetry ordering. FIFO eviction,
disabled capacity, first samples, saturation, telemetry and slow logs retain
their contracts. No facade API, dependency, persistent format or default changes.

## Real facade allocation measurements

The existing [query identity probe](../query_identity_2026_10_27/query_identity_probe.rs)
executes ordinary and concurrent public APIs, an equality index, 64 synthetic
nodes, 32 warmup calls and 256 measured point/update calls or 16 full scans.
Three alternating process pairs per case/facade produce 36 records in
`facade.jsonl.gz`. Every call checks row counts; point/update also check exact
returned values. These are allocation measurements, not latency measurements.

| Facade / case | Requests per call before / after | Requested bytes saved per call |
| --- | ---: | ---: |
| Ordinary point | 484 / 478 | 283 |
| Concurrent point | 497 / 491 | 283 |
| Ordinary scan | 785 / 779 | 273 |
| Concurrent scan | 798 / 792 | 273 |
| Ordinary update | 1,267.03515625 / 1,261.03515625 | 308 |
| Concurrent update | 1,181 / 1,175 | 308 |

Each call removes five metadata allocations and one digest lookup clone.
Read samples return to their initial live requested bytes; mutation samples
have identical baseline/candidate retained-byte deltas. Summary ownership and
capacity do not grow to obtain this benefit. Requested allocator bytes exclude
allocator rounding, foreign heaps and RSS.

## Current owned-input controls

The manual [allocation example](../../../examples/statement_summary_allocations.rs)
compares current owned and borrowed input paths in one binary, after 65 warmup
operations. Three alternating pairs of 1,000 calls produce 36 component records.
This owned control already includes the new borrowed-key lookup; it is not the
historical baseline above. Clock fields alone are normalized for snapshot
equality, and every allocation window has zero live requested-byte delta.

| Case | Requests owned / borrowed | Requested bytes owned / borrowed |
| --- | ---: | ---: |
| Hot point summary | 5 / 0 | 234 / 0 |
| Hot 16 KiB UTF-8 literal | 5 / 0 | 4,241 / 0 |
| Disabled summary | 5 / 0 | 234 / 0 |
| Cold insertion including owner reset | 10 / 10 | 3,203 / 3,203 |
| Capacity-one alternating-digest churn | 8 / 8 | 402.5 / 402.5 |
| Failed hot execution, owned fallback | 7 / 7 | 268 / 268 |

## Existing admitted-host controls

The unchanged `host_boundary` / `host_boundary_allocations` benches retain 112
records: seven alternating pairs across four cases and separate instrumented
allocation versus uninstrumented timing binaries. All schemas/counts/checksums,
allocation requests, requested bytes, resize counts and live-byte deltas agree.
Admitted read transactions do not record statement summaries. Bulk mutation
records its first sample, so it has no successful summary hit to optimize.

Uninstrumented query-boundary medians, nanoseconds, are retained without a
latency claim; all four candidate medians are worse and sample ranges overlap:

| Case | Baseline | Candidate |
| --- | ---: | ---: |
| One cold indexed point | 74,375 | 75,167 |
| 128 indexed points | 4,336,672 | 4,528,588 |
| 4,096-row scan | 3,574,292 | 3,695,417 |
| 512-row bulk insertion plus scan | 3,484,667 | 3,577,917 |

These are shared-workstation dev-profile observations, not release throughput,
p99, RSS, storage durability or remote CI speedup evidence. Negative samples
are preserved. The baseline first build cost 327 seconds; the measured candidate
host rebuild took 7.40 seconds. Separate facade probe builds took 11.07/8.91
seconds. Compilation is outside measurement windows.

## Replay and provenance

Use separate baseline/candidate checkouts and the pinned toolchain. Temporarily
copy the existing `query_identity_probe.rs` into each checkout's `examples/`,
then build it and the two existing host-boundary benches from that root:

```console
env CARGO_BUILD_JOBS=2 cargo build --locked --example query_identity_probe --bench host_boundary --bench host_boundary_allocations
```

Copy the executables to one scratch directory as `baseline-probe`,
`baseline-timing`, `baseline-allocations`, and the corresponding `candidate-*`
names. Bench executables are under `target/debug/deps/` with Cargo hash suffixes.
Build the allocation example in the candidate and copy its executable there as
`candidate-component`:

```console
env CARGO_BUILD_JOBS=2 cargo build --locked --example statement_summary_allocations
python3 docs/benchmarks/statement_summary_2026_10_27/replay.py /path/to/binaries /path/to/fresh-output
```

The measured admitted baseline was built before source edits. For the facade
baseline, the two changed production files were byte-restored from the baseline
revision inside the isolated worktree, built with the identical temporary probe,
then restored to the candidate and rebuilt. All other production Rust sources,
dependencies/features/profiles were unchanged. Temporary developer-only Cargo
benchmark declarations were subsequently removed; rebuilding with the final
manifest reproduced both measured candidate host binaries byte-for-byte. The
final allocation example also reproduced all 36 component records exactly.

Raw JSONL is gzip-compressed beside this file. Console/build/test logs remain
outside the repository. No benchmark or fuzz campaign is registered in CI.
