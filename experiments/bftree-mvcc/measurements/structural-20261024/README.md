# Bf-Tree internal keys, batches and snapshot-safe maintenance

Manual component evidence for [#1237](https://github.com/nowledge-co/hawdb/issues/1237), within [PR #1213](https://github.com/nowledge-co/hawdb/pull/1213). Source base: `8b0744932565099dcff60c36c3034a28c1ea8ad8`; final Rust sources are bound by the per-file hashes below. This report does not select a production backend.

The default adapter now offers bounded ordered batches, explicit snapshot-safe collection/checkpoints, and old receipt payloads read by captured WAL range instead of retained forever in RAM. Native head/separator candidates are working, tested probes; the default remains the original registry dependency. Collection substantially accelerates Versioned full scans, but costs foreground synchronized work, more journal space and higher observed post-maintenance RSS. Batches help duplicate-heavy and some adjacent Hybrid requests; sparse/missing requests and retained-history Versioned batches can be slower.

## Method and scope

- Apple M3 Max ARM64, 16 CPUs, 128 GiB, macOS 27.0.1, local APFS. Rust 1.97.1 and the repository global release profile; no private profile, target-CPU or compiler-flag override.
- Bf-Tree 0.5.6, upstream commit `3998ce8643dec4e98b07f2d11cad67bf2c2d28d1`. Memory backend, native WAL/CPR disabled, the same 4 MiB cache configuration and disabled record caching in all Bf cases. The cache setting is not a process-memory limit.
- Every complete transaction and maintenance record uses `sync_all`. Values are 100 bytes, seed batches at most 128 keys, update batches at most 64. No group commit or weakened acknowledgement boundary.
- Five consecutive matched repetitions rotate variants/layouts and reverse order. Representative cases: numeric8 at 16/2, 512/24 and 2048/32 keys/update rounds; prefix64 and spread64 at 512/24. Diagnostics additionally cover both long shapes at 2048/32. Those larger long-key diagnostic timings are not qualification.
- `prefix64` has 56 common bytes plus an ordered counter. `spread64` has the ordered eight-byte counter followed by 56 varying bytes; it is not a uniformly random UUID workload. Advanced tests populate even IDs and use in-range odd IDs for misses.
- Native and old/new comparisons verify 4096 standalone point reads and 32 complete ordered scans for each current/old snapshot. Advanced tests additionally verify three read phases and 32 matched scalar/batch requests for each dense/sparse/missing/duplicate category. Every value and row order is asserted.
- The read-only auditor compares actual normalized transaction bytes: epochs, operation IDs and every complete mutation. Every matched native quartet also has byte-identical journals. Magic 01 versus 02 and maintenance records legitimately change other raw journal hashes.
- Ratios below are medians of five per-pair ratios; below 1 means lower time or resource use. Batch times are per request, not per key. Scan/batch p99 is a maximum of only 32 samples. Sub-microsecond point percentiles are quantized on this host; small percentage changes are not stable production-tail estimates.
- COW is a component reference under the same adapter/journal, not an end-to-end comparison with HawDB transactions or Mem routes. Shared-host compiler and load observations are retained; they do not prove continuous quiescence.

| Mode | Processes | Matched groups | Compiler observed at a boundary | One-minute load range | Child wall time, s |
| --- | ---: | ---: | ---: | --- | ---: |
| diagnostic | 24 | 6 | 24 | 10.87–26.66 | 186.89 |
| maintenance | 150 | 75 | 54 | 4.36–12.58 | 405.48 |
| native | 300 | 75 | 23 | 4.27–19.10 | 621.35 |
| baseline | 150 | 75 | 142 | 8.19–26.56 | 359.83 |

The final cohorts contain **600 uninstrumented timing processes and 24 separate diagnostic processes**, 192,240 timed commit batches, 5,529,600 standalone point reads and 43,200 full scans. All 231 normalized transaction groups pass; 75 native quartets also match raw journal bytes. No adverse sample is removed.

## Internal keys: measurements and working candidates

The native probe counts real pivot bytes and preview/postfix comparisons. It captures owned summaries under existing node mutation protection and never walks unprotected native pointers. Counts are process-wide, including background workers and all live generations. Operand spans bound possible comparison bytes; they do not count actual machine comparisons. Diagnostic timings are excluded from every timing conclusion.

`head` uses big-endian four-byte comparisons and skips repeated ineffective previews when a sufficiently populated node has equal sorted endpoint previews matching the search. `compact` chooses a byte separator satisfying `left_max < separator <= right_min` before installing both leaf fences and the parent pivot. The root split stays upstream. `combined` applies both. Shortening only the parent pivot would misroute gap insertions; these probes change the actual split fence. Helpers cover binary/proper-prefix ordering, and native tests cover shuffled variable-length keys, deep histories, GC and replacement generations.

One diagnostic run per shape/layout at 2048/32, immediately after writes:

| Shape/layout | Original mean pivot bytes | Compact mean pivot bytes | Original/compact live inner nodes | Compact/original pivot bytes | Original equal-preview fraction |
| --- | ---: | ---: | ---: | ---: | ---: |
| numeric8/versioned | 26.00 | 25.45 | 40/35 | 0.993 | 0.99986 |
| numeric8/hybrid | 25.22 | 24.63 | 36/37 | 0.997 | 0.72414 |
| prefix64/versioned | 138.00 | 136.76 | 179/182 | 0.992 | 0.99986 |
| prefix64/hybrid | 137.48 | 136.11 | 223/199 | 0.989 | 0.77125 |
| spread64/versioned | 138.00 | 97.35 | 313/243 | 0.719 | 0.99987 |
| spread64/hybrid | 137.36 | 89.53 | 343/241 | 0.652 | 0.73735 |

Fixed previews are usually inconclusive in these ordered-prefix inputs. Head shortcuts remove most redundant preview comparisons in homogeneous Versioned nodes, but they do not remove the remaining postfix comparisons. Many retained-version split boundaries still need epoch bytes; they cannot all become tiny logical-key separators. Longer shared-prefix fences also constrain how much can be removed. Shorter fences can reduce the existing leaf-prefix compression benefit.

Current/old full-scan p95 ratios versus temporary uninstrumented upstream; all three layouts and all five cases are retained, including unchanged COW controls:

| Case (keys/rounds/shape/layout) | Head current/old | Compact current/old | Combined current/old |
| --- | ---: | ---: | ---: |
| 16/2/n8/versioned | 1.023/0.866 | 0.993/0.927 | 1.040/0.967 |
| 16/2/n8/hybrid | 1.256/0.906 | 1.345/1.015 | 1.423/0.920 |
| 16/2/n8/cow | 0.941/1.333 | 0.951/2.001 | 0.915/2.373 |
| 512/24/n8/versioned | 0.972/0.976 | 0.998/0.979 | 1.002/0.988 |
| 512/24/n8/hybrid | 0.994/0.943 | 1.119/1.131 | 1.011/0.939 |
| 512/24/n8/cow | 1.028/1.127 | 1.028/1.015 | 1.058/1.057 |
| 512/24/p64/versioned | 0.966/0.979 | 0.915/0.959 | 0.946/1.011 |
| 512/24/p64/hybrid | 0.974/0.983 | 1.008/1.216 | 1.005/1.134 |
| 512/24/p64/cow | 1.034/1.206 | 1.083/1.211 | 1.230/1.308 |
| 512/24/s64/versioned | 0.985/1.030 | 0.981/0.931 | 0.923/0.911 |
| 512/24/s64/hybrid | 1.024/0.965 | 0.996/0.796 | 0.964/0.731 |
| 512/24/s64/cow | 0.957/1.022 | 0.901/0.891 | 0.936/0.947 |
| 2048/32/n8/versioned | 0.943/0.965 | 0.944/0.937 | 0.924/0.924 |
| 2048/32/n8/hybrid | 1.011/0.968 | 1.028/1.042 | 0.980/0.972 |
| 2048/32/n8/cow | 1.198/1.178 | 1.180/1.110 | 1.249/1.424 |

The combined Hybrid spread-key old scan ratio is 0.731, while combined Hybrid common-prefix old scans are 1.134 and compact is 1.216. These are workload-specific observations. Unchanged COW controls also move, and synchronized write-tail ratios vary substantially. No native candidate is selected by default; these data do not qualify a universal comparator/fence improvement. All point/write/reopen/memory metrics and per-pair ratios remain in the complete summaries.

## Batches: position reuse and CPU costs

`get_many` bounds inputs to 128 keys, normalizes order/duplicates, carries encoded targets across returned records, releases guards between native windows and before Hybrid history probes, and falls back when intervening records are unproductive. Exact equality with a generated valid key avoids decoding it again. Native deleted slots can be skipped internally: 32 returned records is not a hard raw-slot or latch-duration bound.

The initial implementation repeated target encoding and key parsing; the complete exploratory observations are preserved below. The final implementation removes those costs. Native counters independently verify reuse. For 128 adjacent Hybrid prefix64 current keys at 2048/32, the median internal `lower_bound` calls per request in the final diagnostic run are:

- Scalar: **393**; batched: **60**. These count native internal-node calls, not exactly root descents. Deep-history Versioned batches show little descent reduction until collection; duplicates reduce probes much more.

Final current/old request p95 ratios, batch divided by matched scalar, for 512/24 with maintenance disabled. Duplicate and missing results preserve the original request multiplicity/order. Larger and smaller cases and after-maintenance phases are also retained in the complete data.

| Shape/layout | Current dense | Current sparse | Current mixed miss | Current duplicate | Old dense | Old duplicate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| numeric8/versioned | 1.197 | 1.119 | 1.159 | 0.325 | 1.171 | 0.272 |
| numeric8/hybrid | 0.902 | 1.216 | 1.250 | 0.440 | 1.056 | 0.201 |
| numeric8/cow | 1.530 | 1.521 | 1.604 | 0.747 | 1.537 | 0.563 |
| prefix64/versioned | 1.209 | 1.117 | 1.122 | 0.319 | 1.128 | 0.236 |
| prefix64/hybrid | 0.948 | 1.280 | 1.224 | 0.427 | 1.012 | 0.186 |
| prefix64/cow | 1.526 | 1.438 | 1.482 | 0.667 | 1.440 | 0.552 |
| spread64/versioned | 1.403 | 1.144 | 1.105 | 0.321 | 1.084 | 0.173 |
| spread64/hybrid | 1.098 | 1.208 | 1.169 | 0.434 | 0.990 | 0.145 |
| spread64/cow | 1.447 | 1.299 | 1.372 | 0.716 | 1.469 | 0.550 |

The optional batch API is useful for duplication and some locality. It is not a universal substitute for point reads: sparse/missing requests generally pay normalization and failed cursor-work costs, and retained-history Versioned batches can be slower. COW dense batches expose the normalization cost without native cursor benefits. Nothing automatically routes production requests through this experiment.

## Snapshot-safe collection, checkpoints and recovery

All read and write transactions register their captured epochs before releasing the capture gate. A fixed-horizon bounded pass retains the newest anchor at or below the oldest lease and all newer versions, conservatively including tombstones. Lease release schedules another pass when needed. A complete GC plan is synchronized before physical deletion; errors/unwinds poison handles and reopening replays it completely.

A complete checksummed checkpoint is synchronized before publishing a replacement index generation. Existing views retain their own generation. Recovery validates the complete prefix, restores the last complete checkpoint and rebuilds only the suffix. Exact operation-ID retry retains the original mutation evidence on disk and validates a captured record interval; current heads alone retain full receipt payloads in RAM. No receipt hash replaces equality.

`collect`/`keep` ratios after releasing the old views. Retained counts are ledger version epochs: they correspond to required Bf versions, but are **not COW physical map entries**; COW snapshots already own immutable maps. RSS is actual process resident memory, separate from ledger/payload counts.

| Case | Retained epochs before→after | Current scan p95 ratio | Reopen ratio | RSS ratio | WAL size ratio | Maintenance median, s | GC step p99 median, ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 16/2/n8/versioned | 48→16 | 0.552 | 0.913 | 1.029 | 2.512 | 0.016 | 4.658 |
| 16/2/n8/hybrid | 48→16 | 1.038 | 0.629 | 1.026 | 2.512 | 0.015 | 4.418 |
| 16/2/n8/cow | 48→16 | 1.200 | 0.985 | 1.007 | 1.817 | 0.016 | 5.391 |
| 512/24/n8/versioned | 12800→512 | 0.082 | 0.435 | 1.887 | 2.261 | 0.517 | 7.844 |
| 512/24/n8/hybrid | 12800→512 | 0.923 | 0.275 | 1.893 | 2.261 | 0.539 | 9.163 |
| 512/24/n8/cow | 12800→512 | 1.041 | 0.232 | 1.071 | 1.239 | 0.538 | 6.121 |
| 512/24/p64/versioned | 12800→512 | 0.077 | 0.539 | 1.936 | 2.504 | 0.591 | 10.933 |
| 512/24/p64/hybrid | 12800→512 | 0.992 | 0.345 | 2.011 | 2.504 | 0.540 | 9.097 |
| 512/24/p64/cow | 12800→512 | 1.000 | 0.276 | 1.089 | 1.502 | 0.551 | 10.815 |
| 512/24/s64/versioned | 12800→512 | 0.071 | 0.436 | 1.953 | 2.504 | 0.527 | 5.969 |
| 512/24/s64/hybrid | 12800→512 | 0.987 | 0.283 | 1.991 | 2.504 | 0.527 | 7.304 |
| 512/24/s64/cow | 12800→512 | 1.027 | 0.241 | 1.069 | 1.502 | 0.530 | 5.761 |
| 2048/32/n8/versioned | 67584→2048 | 0.026 | 0.452 | 1.752 | 2.252 | 2.848 | 10.125 |
| 2048/32/n8/hybrid | 67584→2048 | 0.872 | 0.283 | 1.740 | 2.252 | 3.742 | 16.148 |
| 2048/32/n8/cow | 67584→2048 | 1.044 | 0.321 | 1.190 | 1.219 | 2.767 | 10.673 |

For large Versioned full scans, an illustrative amortization estimate is about **450 additional complete scans** to cover this measured maintenance cost, using each pair's mean per-scan saving. This is not a workload-independent trigger or a claim about bounded Mem candidate pages. The host must account for foreground maintenance and its own read mix.

Costs and limits are material:

- Bf RSS after maintenance is about 1.74–2.01 times the matched no-maintenance control in retained-history cases, despite fewer retained index versions. Checkpoint rebuilding allocates another tree and large temporary images; these observations do not prove a total-memory reduction or identify allocator retention versus other residency costs. Peak and post-maintenance RSS are retained separately.
- The journal grows because it preserves old exact-retry evidence, GC plans and complete checkpoint images. Large numeric Bf cases grow from approximately 7.75 MB to 17.44 MB. This is index-version collection, **not disk-capacity reclamation**; the 32 MiB cap still refuses growth.
- Checkpoints are explicit foreground work, not background compaction. Old leases can block collection. No durable branch, TTL, spill or production pruning policy is added.
- A checkpoint does not authorize ignoring corrupt earlier evidence. Full-prefix validation, receipt metadata rebuilding and unsynchronized lost-response resolution remain recovery costs. Torn/corrupt/extra evidence fails closed and remains intact.

## Final default versus preceding adapter, without collection

Baseline is the already published `8b0744932` upstream-code executable (the earlier probe’s checksum-verified copy of registry source, without a comparator patch), whose hash matches the preceding SIMD protocol. The final default uses registry Bf-Tree and the new journal grammar. Both synchronize every complete transaction; normalized transaction content matches exactly. This comparison captures receipt/recovery/capture changes, not only a comparator change. The driver calls the final default `upstream` in this mode.

| Case | Commit p99 ratio | Current/old scan p95 ratio | Reopen ratio | RSS ratio |
| --- | ---: | ---: | ---: | ---: |
| 16/2/n8/versioned | 1.013 | 0.915/1.096 | 1.324 | 1.012 |
| 16/2/n8/hybrid | 0.997 | 0.773/1.030 | 1.096 | 1.017 |
| 16/2/n8/cow | 0.960 | 1.000/1.242 | 1.164 | 1.016 |
| 512/24/n8/versioned | 0.696 | 0.984/0.995 | 1.156 | 0.879 |
| 512/24/n8/hybrid | 0.993 | 1.037/1.094 | 1.365 | 0.879 |
| 512/24/n8/cow | 0.695 | 1.122/1.036 | 1.092 | 0.809 |
| 512/24/p64/versioned | 0.942 | 1.054/1.034 | 1.253 | 0.858 |
| 512/24/p64/hybrid | 0.769 | 0.992/0.998 | 1.324 | 0.854 |
| 512/24/p64/cow | 1.199 | 1.003/1.018 | 1.053 | 0.772 |
| 512/24/s64/versioned | 0.703 | 0.960/0.978 | 1.187 | 0.881 |
| 512/24/s64/hybrid | 1.539 | 0.961/0.964 | 1.287 | 0.881 |
| 512/24/s64/cow | 1.108 | 1.207/1.035 | 1.107 | 0.774 |
| 2048/32/n8/versioned | 0.962 | 1.074/1.002 | 1.125 | 0.776 |
| 2048/32/n8/hybrid | 1.134 | 1.050/1.089 | 1.277 | 0.784 |
| 2048/32/n8/cow | 1.028 | 1.084/1.106 | 1.076 | 0.568 |

Before maintenance, large-case RSS is about 0.776/0.784/0.568 of the previous Versioned/Hybrid/COW adapter, consistent with no longer holding every old receipt payload. Without a checkpoint, large-case reopening is slower (ratios 1.125/1.277/1.076): metadata validation plus index reconstruction remain two passes. This cohort has compiler observations at 142/150 boundaries and unchanged COW scan controls also move; synchronized write-tail and small timing differences do not establish causal regressions or gains. The complete adverse results are preserved.

## Correctness, faults and source binding

- Final default and each actual upstream/head/compact/combined snapshot, both diagnostic and uninstrumented, pass **40 tests in checked and release profiles** and strict all-target/all-feature Clippy. Both ignored crash children are invoked by parent tests. The source audit verifies 486 prepared source hashes and 562 local dependency files; the final uninstrumented adapter sources match the repository.
- Coverage includes binary/prefix/missing/duplicate/tombstone semantics; shuffled variable-length keys across inner levels; active read/write leases; anchor retention; old-writer conflict; partial collection; checkpoint replacement; suffix replay; exact old retries; and reader/writer/collector races.
- Subprocess tests cover nine transaction boundaries/layouts and 15 maintenance boundaries/layouts. Checksummed invalid GC/checkpoint metadata, covered-prefix corruption, lost unacknowledged tails, torn/zeroed writes and reordered complete blocks retain evidence and fail closed where required.
- Repeated verification found a close/reopen `Busy` race. A deterministic copied-descriptor regression fails before the fix. The final journal owner explicitly unlocks on final logical-handle drop; live snapshots and the next owner remain exclusive. No retry/sleep weakens the ownership assertion. Original failure and fixed test logs are retained.
- Existing root captured-range tests: **3 passed**. Root fmt and strict workspace/all-target/all-feature Clippy pass; independent fmt and strict Clippy pass. The read-only HawkEye/fmt/Clippy hooks pass for all owned files and are rerun at commit.
- Required local Bazel command passes **98 targets: 43 executed, 55 cached**, 2338.362 s total elapsed under concurrent compilation. The standalone production MVCC campaign completes **3,717 cases in 30,036 ms**. Neither production campaign covers the isolated adapter.
- `prek run --all-files` retains **624 historical license-header failures**; it is not represented as a clean full-tree audit. No lint suppression, compiler-profile override or weakened check is introduced.
- Production workspace/normal dependencies and library archive exclude the experiment/Bf-Tree. Only the existing low-level `BinaryWalReader::range` visibility/documentation changes in production storage, to expose the captured-interval reader; its algorithm and canonical format do not change. No facade integration, CI or scheduled experiment lane is added.
- Temporary upstream copies expose the existing nine `cfg(spdk)` warnings. Strict adapter/root Clippy remains enabled; no unsupported feature is invented to silence third-party configuration warnings.

The platform assumption is that successful synchronization provides the filesystem's documented durability. Process exits plus deterministic fault images do not qualify real storage-device write caches, power loss or native Windows/Linux execution. `BFMVCC02` is a greenfield experiment identity requiring recreation of older experiment databases, not a canonical HawDB format migration. Snapshot isolation still permits write skew.

Build costs are recorded by stage in [build receipts](build-receipts.json.gz) and full logs. First observed copied-workspace timings are not cold-build comparisons: dependencies/caches were reused and cargo lock waits, compilation and test execution are distinct. In the first cohort, upstream dev-test/release-test/release-build stages were 37.37/57.39/66.56 s; the later final source is separately recorded. No local result establishes a remote CI speedup.

## Artifacts and reproduction

- [Audit and counts](artifact-audit.json), [protocols and executable/driver hashes](protocols.json.gz), [source cards and deduplicated exact harness snapshots](source-snapshots.json.gz), [build receipts](build-receipts.json.gz), [complete validation logs](validation-logs.json.gz).
- [Native raw observations](native-results.jsonl.gz) and [complete numeric summary](native-summary.json.gz); [maintenance raw observations](maintenance-results.jsonl.gz) and [summary](maintenance-summary.json.gz); [preceding-adapter raw observations](baseline-results.jsonl.gz) and [summary](baseline-summary.json.gz).
- [Diagnostic snapshots/counters](diagnostic-results.jsonl.gz) and [unqualified diagnostic summary](diagnostic-summary.json.gz). Diagnostic times must not be used as performance qualification.
- [Exploratory initial batch cohort](exploratory-initial-batch-results.jsonl.gz) and [its protocol](exploratory-initial-batch-protocol.json.gz): 51 successful children plus one explicitly stopped child, retained when duplicate target encoding/parsing was found. It is not a completed five-pair result and is not merged into final statistics. The source cards also retain the later before-owner-fix snapshot and failing parallel-test evidence.

Use the [manual prepare/build/measure drivers](../../structural/README.md) from the repository root. Use new scratch/result/journal directories, the unchanged global release profile, five matched repetitions, the frozen default-registry binary for maintenance/baseline, and a verified `8b0744932` executable for the optional old/new mode. Every final protocol records the executed driver, source-card and binary hashes. Complete per-pair values are kept alongside medians; the larger long-key cases remain diagnostic scope unless `--matrix full` is selected.
