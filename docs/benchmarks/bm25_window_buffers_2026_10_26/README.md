# BM25 bounded windows and shared blocks

Baseline production revision: `bb69b33664fc7523997c670d57df661e8ff204fd`.
The baseline has only the candidate's regression/measurement fixtures added.
Candidate source and binary identities are recorded in `provenance.json`.
The pinned Rust 1.97.1 repository test profile, locked dependencies, all Search
features and an Apple Silicon macOS host were used for both revisions.

These are synthetic allocation and read-volume probes, not release latency,
throughput, process RSS, cold-cache or real-export qualification. Fixture
construction is outside allocation observation. The two allocation probes use
the existing serial allocator gate; no query elapsed-time claim is made.

| Workload / counter | Baseline | Candidate |
| --- | ---: | ---: |
| Two artifacts, Top-1, matching documents | 2 | 2 |
| Retained candidate IDs | 2 | 1 |
| Retained candidate ID bytes | 8,194 | 4,097 |
| Whole score-collection allocation peak, bytes | 15,548 | 15,404 |
| Whole score-collection bytes read | 10,442 | 10,442 |
| Two 65,537-byte IDs, full ranks plus two-entry window: allocation peak, bytes | 263,012 | 560 |
| Eight documents / three co-located query terms: scoring-phase posting bytes read | 1,137 | 379 |

The score-collection peak includes statistics, mapping and current-segment
workspace, so halving the retained ID payload does not halve the whole-query
peak in this tiny fixture. The read-sharing regression separately excludes
statistics and document-mapping reads; it asserts all 24 postings, eight
matches and exact reference BM25 score bits.

The unchanged regression assertions fail on the baseline: its local-window
union contains two entries for a global Top-1, and it reads the same 379-byte
posting block three times. Both pass on the candidate. Additional guards cover
pagination, zero/one/large windows, deterministic ties, accounted ownership,
duplicate live layers, cancellation on cached reads, checksum-failure retry,
and inherited Block-Max floors before the local heap fills.

## Mechanism and scope

Each disjoint visible artifact contributes its local Top-K under full live-corpus
statistics. Merging each local window into one global heap retains at most K
entries between artifacts, with one current local window overlapping it during
admission. Unbounded and complete producers retain their existing complete
semantics. Possible overlapping ID ranges use exact membership checks rather
than relying on range bounds as proof that documents belong to one layer.
These overlap checks can add mapping-block reads for retained local candidates;
the disjoint two-artifact allocation fixture does not qualify that cost. No
latency or read-volume improvement is claimed for overlapping artifact ranges.

Eligible hybrid scoring inherits the previous artifacts' global Kth score.
This can activate Block-Max WAND before the current local window fills. Bounds
remain scoped to their current frames, accumulated in scorer term order, and
compared strictly below the floor so score ties remain eligible. Ordinary text
queries with exact hit totals and complete text producers do not prune.

Term cursors share one reusable encoded-block buffer per scoring call, caching
the last validated physical block. Different term frames retain independent
decoded posting vectors. A cancelled cache hit still refuses; a failed read is
never installed as a valid cache entry. Buffer admission charges one encoded
workspace while preserving decoded-vector and transition charges. This adds
no persistent or process-wide cache and changes no artifact format.

Current Block-Max bounds live inside blocks: the reader still reads a block
before consulting its frame bounds. WAND primarily saves decoding/scoring;
shared buffering avoids repeated reads for co-located terms. A separate
pre-read block-upper-bound index is future work, not established by this probe.

Ranks and their window maps borrow IDs from retained score maps. Output owners
still admit their own copies. For bounded hybrid results, child components now
follow the global child window consistently across one or multiple artifacts:
a hit outside that window has a zero child score, instead of retaining a
segment-local diagnostic score. The single-versus-layered regression covers
IDs, final scores, raw child components and hit counts for windows 0/1/2.

## Replay

On each frozen source with the same two ignored measurement functions:

```console
cargo test --locked -p hawdb-search --all-features --lib -- bm25_window_allocation_measurement rank_id_allocation_measurement --ignored --nocapture --test-threads=1
cargo test --locked -p hawdb-search --all-features --lib -- bounded_segment_scores_merge_into_one_global_window lexical_query_shares_a_physical_block_between_terms --test-threads=1
```

The second command must fail on the baseline and pass on the candidate.
`docs/TEST_INDEX.md` records ordinary feature-matrix ownership. These ignored
allocation probes are manual-only and are not CI performance gates.
