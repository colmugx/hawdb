# Query identity encoding allocation experiment

Baseline: `b6de4430e935c540ec3644ede593e4c72da27872`. The candidate is
identified by source SHA-256 values in [manifest.json](manifest.json), rather
than a self-referential commit ID. Measurements use Rust 1.97.1 on
`aarch64-apple-darwin`, on a shared workstation.

The candidate borrows trimmed language names when ASCII lowercasing is
unnecessary, reserves the exact versioned digest length, and writes lowercase
hex directly. Hash input framing and both `q1`/`t1` protocols are unchanged.
This removes eight digest-buffer resize requests and, for canonical language
names, one temporary language allocation per identity.

## Component measurements

The manual unpublished probe links frozen baseline and candidate implementations
into one binary. It uses the repository release profile, 64 warmup calls and
five alternating producer pairs per case. Allocation samples use 1,000 calls;
separate uninstrumented timing samples use 10,000. The allocator counter is
owned by the probe executable, never the production library.

| Identity case | Allocation requests before / after | Requested bytes before / after | Retained requested bytes before / after |
| --- | ---: | ---: | ---: |
| Parameterized Cypher point query | 17 / 8 | 2,654 / 2,286 | 688 / 566 |
| Parameterized SQL | 16 / 7 | 1,613 / 1,248 | 418 / 296 |
| Mixed-case language | 17 / 9 | 2,654 / 2,292 | 688 / 566 |
| Empty query | 11 / 2 | 502 / 134 | 256 / 134 |
| 16 KiB literal | 15 / 6 | 1,346 / 978 | 428 / 306 |

All nine cases retain 122 fewer requested bytes per newly constructed identity:
each digest's capacity falls from 128 to 67 bytes. Loop live-byte deltas and
post-drop retained deltas are zero. The unchanged plan-cache normalizer has
identical allocation requests, requested bytes and resize counts in all cases.
Requested allocator bytes do not measure allocator rounding or RSS.

Uninstrumented component medians, nanoseconds per constructor:

| Case | Baseline | Candidate |
| --- | ---: | ---: |
| Parameterized Cypher point query | 2,046 | 1,441 |
| Parameterized SQL | 1,289 | 661 |
| Mixed-case language | 2,039 | 1,477 |
| Empty query | 747 | 137 |
| 16 KiB literal | 27,823 | 28,253 |

The long-literal negative sample is retained. These component timings do not
establish general query throughput or tail-latency improvement.

## Admitted embedded-host controls

The existing `host_boundary` and `host_boundary_allocations` Cargo benches run
with default features and the repository dev profile. Seven alternating process
pairs cover in-memory indexed point reads (128 statements), a 4,096-row scan
(one statement), and a 512-row bulk insertion plus complete verification scan
(two statements). Synthetic rows have integer IDs, float scores and UTF-8
titles. Every result schema, row count and checksum agrees.

| Case | Allocation requests before / after | Requested bytes before / after | Uninstrumented median query ns before / after |
| --- | ---: | ---: | ---: |
| 128 point statements | 67,073 / 65,921 | 8,593,014 / 8,545,910 | 7,864,661 / 7,291,788 |
| 4,096-row scan | 33,191 / 33,182 | 43,484,163 / 43,483,795 | 6,323,542 / 6,061,166 |
| 512-row bulk insert plus scan | 50,732 / 50,714 | 11,555,596 / 11,554,860 | 5,670,957 / 5,711,126 |

Savings are fixed per statement, not per returned row: nine allocation requests
and 368 requested bytes per canonical Cypher identity. The bulk timing is
slightly worse and sample ranges overlap; no overall latency claim is made.
These memory-backend controls do not qualify persistence or power-loss safety.

First dev engine compilation cost 303 seconds; the candidate rebuild took
6.47 seconds. The initial release component build took 6.97 seconds; subsequent
vector, allocation and timing rebuilds took 4.83, 2.39 and 2.04 seconds.
Build costs are separate from measured operations and remote CI performance.

## Artifacts and replay

The compressed JSONL files retain all samples, including negative timing
samples: 270 component allocation/retention records, 180 component timing
records, nine exact protocol vectors and 84 admitted-host records.
`manifest.json` records source, binary, job and compiler identity. Console/build
logs remain outside the repository. No benchmark is registered in CI.

From the candidate checkout, run the standalone component replay with a fresh
output directory. It verifies source hashes, extracts the baseline from Git,
uses the pinned lockfile and preserves the repository-wide Cargo profiles:

```console
python3 docs/benchmarks/query_identity_encoding_2026_10_27/replay.py /private/tmp/identity-encoding-replay
```

For admitted controls, build and copy both existing benches from each revision
before timing anything. Keep local compiler/test workloads idle while measuring.
Name the four binaries `baseline-timing`, `baseline-allocations`,
`candidate-timing`, and `candidate-allocations` in one directory, then pass it
to `--host-binaries`. The replay generates the same synthetic jobs and alternates
seven pairs. Retained binaries are local evidence, not committed executables.

```console
env CARGO_BUILD_JOBS=2 cargo build --locked --bench host_boundary --bench host_boundary_allocations
python3 docs/benchmarks/query_identity_encoding_2026_10_27/replay.py /private/tmp/identity-encoding-replay-full --host-binaries /private/tmp/identity-encoding-binaries
```

The component replay asserts exact normalized text, cache keys and protocol
bytes across implementations. An independent Python SHA-256 check reconstructs
length-prefixed UTF-8 fields and the little-endian version from the vectors.
Engine regressions remain owned by the existing query/root CI targets; this
manual measurement is additional evidence.
