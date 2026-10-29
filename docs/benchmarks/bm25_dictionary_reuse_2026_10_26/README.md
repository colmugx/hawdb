# BM25 verified dictionary reuse, 2026-10-26

The current writer flushes a physical posting block at **128 total entries**
(`ArtifactBuilder::push_posting_inner`), and `encode_postings` encodes one frame
per term in that block. Its query decoder already materializes at most 128
postings per term per block. Frame cursors alone cannot reduce that bound for
current writer output. Supporting larger physical blocks would be separate work.

This experiment instead reuses the validated FST while the shared physical block
remains loaded. The FST owns the existing block Vec through an `AsRef<[u8]>`
dictionary range. Eviction destroys the view and recovers that Vec before
replacing its bytes. There is no dictionary copy or second encoded allocation.
Term cursors borrow the query buffer directly, removing their unused private
buffer fields. Buffer admission, checksum/panic handling, BM25 arithmetic,
pruning and the on-disk format retain their existing contracts. Ref #1278.

## Procedure and source binding

Baseline: main `84d5ed46efa6e05531206c5a4da9782479f1f13a`, with only the
common manual probe, its parity guard and a test-only FST-verification counter
added. Candidate: this PR's working tree on the same base, bound by the source
hash below. These are checked test binaries, not release latency measurements.

| Build | SHA-256 of lexical_projection.rs | SHA-256 of frozen test binary |
| --- | --- | --- |
| baseline | `5f7504fe600e809197738096341aa2a2d37de149780a1f17743fc5387907d58d` | `30d8412535efee489ef8269af87687be5388d6b646f66c710dbcfb1ea503915b` |
| candidate | `c1adc72047f0c48f9e0a95840658a029e593bd72a284ad1e622e5e7391c1fdde` | `5764b0b2cc02cab6990ee4ee65ab91d352b6595f67ac5aeb74959cc086d47e6a` |

Build each revision in a separate checkout and Cargo target directory:

```console
cargo test --locked -p hawdb-search --lib dictionary_reuse_query_measurement --no-run
```

Copy and hash each executable before measuring. Run the immutable binaries with
`dictionary_reuse_query_measurement --ignored --nocapture --test-threads=1` in
order **B C C B B C C B**. Each invocation builds fresh fixtures outside all
measurements, warms each query 32 times, and times 21 batches (32 queries per
batch for 128 documents, 4 per batch for 4096). Report the median of the four
process-level p50 values; the JSON retains every process-level p50/p95.

Environment: macOS arm64, Mac15,8, 16 logical CPUs, 128 GiB RAM, pinned Rust
1.97.1, repository checked Cargo test profile and default Search features.
Single-threaded queries over file-backed synthetic lexical projections with
default limits, no delta, rank window 7, and 1/4/32 queried terms. Document IDs
are `doc-00000000` etc.; document i contains word i modulo term count. Words
are `wordaax`, `wordabx`, etc. (letters plus an x suffix avoid analyzer aliases).
The fixture asserts exactly one posting per document. No durability setting is
changed. Query latency includes statistics and document/posting reads. Fixtures
and this task's compilation finish before timing; the kernel cache is warmed,
not dropped. Other host activity is uncontrolled; storage model was not recorded.

The allocator probe measures one additional query's peak requested live heap
capacity, including the report and cursor vectors, excluding fixture creation
and the caller's already retained state. It is not RSS or a production memory
qualification. Read bytes count library positioned reads including statistics,
not physical device I/O. FST counts include both statistics and scoring.

## Results

| Documents / terms | FST verifications B → C | Read bytes, both | Allocation peak bytes B → C | p50 µs B → C | Candidate timing change |
| --- | --- | --- | --- | --- | --- |
| 128 / 1 | 2 → 2 | 2879 | 7440 → 7216 | 12.481 → 12.535 | +0.43% |
| 128 / 4 | 5 → 2 | 3585 | 7979 → 7755 | 17.787 → 17.739 | -0.27% |
| 128 / 32 | 33 → 2 | 5851 | 21008 → 19216 | 71.044 → 69.227 | -2.56% |
| 4096 / 32 | 64 → 64 | 96349 | 162450 → 160658 | 1329.088 → 1351.037 | +1.65% |

The 32-term shared-block case removes 31 FST verifications per query and lowers
measured heap peak by 1,792 bytes. Its observed p50 improvement is about 2.6%.
The 4096-document control has one full block per queried term, so it has no
verification reuse; its observed p50 is about 1.7% higher despite the smaller
cursor allocation. Single-term and four-term timings are close. These small,
checked-profile timings establish neither a general latency improvement nor
remote CI speedup; verification reduction and unchanged reads are the direct
results. Raw process-level measurements are in [measurements.json](measurements.json).

The ordinary regression compares exact BM25 score bits with the resident
reference, full matching counts, deterministic top-7 ties, zero-allocation
shared hits, Vec reuse at eviction, cancelled hits, and a corrupt internal FST
checksum behind a recomputed valid physical-block checksum. The verification
guard fails on baseline (5 versus 2 checks for four terms) and passes on the
candidate (2 for each of 1/4/32 terms). Existing pruning, filtering, cancellation
and budget/refund tests retain their owners in TEST_INDEX. Test/build logs belong
in the PR evidence, not in this benchmark directory.
