# Internal keys, batched positioning and snapshot-safe collection

Tracking: [#1237](https://github.com/nowledge-co/hawdb/issues/1237), within the
independent experiment in PR #1213. Baseline is `8b0744932`. This document is
the implementation contract; proposed mechanisms are not measured gains.

## Internal key and cursor work

Measure real inner-node separator bytes and preview/full-comparison fallbacks
on numeric, shared-prefix and spread keys. Keep diagnostic counters separate
from uninstrumented timing. Evaluate order-preserving separator/head changes
against the pinned dependency, with binary-prefix, fence, split and concurrent
mutation controls. Preserve the dependency's existing leaf prefix compression.

Add bounded ordered-batch reads with exact input order, duplicates, missing
keys, tombstones and snapshot semantics. Reuse leaf positioning where valid;
fallback when traversal becomes unproductive. Bound records returned to the
adapter and release native guards between batches and before a separate
historical lookup. Bf-Tree may skip native deleted slots internally; returned
record counts do not establish a hard raw-slot or latch-duration bound.

## Snapshot registry and incremental collection

Read and write transactions register their captured epochs atomically with
capture, under a short registry lock. Collection holds the commit coordinator
and this registry while fixing a safe horizon. For each key, retain its newest
version at or before that horizon and every newer version. In particular, do
not delete an old anchor simply because its epoch is below the oldest lease.
Conservatively retain tombstone anchors. Existing snapshots on a replaced
index generation keep their own tree/map ownership.

Each maintenance step examines/removes a caller-bounded number of versions or
keys. A complete checksummed GC plan is synchronized before removing physical
index entries. A lost response may therefore leave a complete durable plan;
partial application/errors/unwinds poison handles and replay applies it fully.
No stale snapshot can start between horizon selection and collection.

## Receipts, checkpoint and replay

The experiment moves to an explicit versioned record grammar. Development
databases may be recreated; no compatibility migration or canonical HawDB WAL
format change is intended.

Retain immutable mutation evidence in the bounded authoritative journal for
exact operation-ID retry. Keep payloads in RAM only while referenced by current
heads, rather than retaining every old batch there. Old retries validate exact
journal payloads, not hashes. This deliberately separates receipt retention on
disk from reclaiming obsolete working-index versions; it does not promise
unbounded operation history or journal-capacity reclamation.

A checkpoint records the complete retained index image, including required old
versions and tombstones, together with its covered epoch and WAL boundary. The
checkpoint is one complete framed record (possibly fragmented), admitted before
append and synchronized before index-generation publication. This keeps the
recovery dependency in the same journal and avoids a cross-file head/rename
protocol. Building a compact replacement index leaves readers on their old
generation until publication. Checkpoint preparation may serialize writers;
its cost belongs in the measurements.

Reopen validates every journal frame, transaction sequence, operation receipt,
GC plan and complete checkpoint. It reconstructs the working index from the
last verified checkpoint and later records; covered transaction payloads remain
receipt evidence, not index versions to resurrect. A torn/corrupt checkpoint or
tail fails closed and remains intact, including after a lost acknowledgement.
Process exits alone do not qualify power-loss behavior. Per-transaction and
maintenance `sync_all` barriers remain explicit and unchanged in strength.

## Verification and acceptance

All three directions remain in scope. Validate current/old batch and scan
results against an independent version model, snapshot capture/collection
races, old writers, retained anchors, delete/reinsert, exact retry after
collection/checkpoint/reopen, bounded incremental work, and failures/crashes at
each durable/partially applied boundary. Keep memory/journal/replay costs and
negative controls in source-bound release measurements. Run root and independent
strict checks and required local fuzz; add no production dependency or CI lane.
