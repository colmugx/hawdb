# HawDB — Native MoonBit SDK

First-class, in-process MoonBit SDK for the HawDB embedded Rust engine.
**No C shim** and **no dependency on `bindings/ffi`**: MoonBit calls
`bindings/moonbit/native` directly via the C calling convention.

## Status

Development SDK, validated by real-engine native CI on Linux x64, macOS ARM64 and Windows x64. Not yet a release
or a claim of parity with the full Python/Go feature set.

## Build and test

From the repository root:

```sh
cd bindings/moonbit
moon check --target native
cd native
cargo build --release
cd ..
rm -rf tests/conformance-db # test-owned fixture directory only
moon run --target native tests/conformance
```

The suite exits with a `HAWDB_MOONBIT_BUSINESS_SUITE_PASS` marker only after
all conformance cases pass; CI rejects `CASE_FAIL`.

## API

Use `open(path)` or `open_in_memory()`; read-only projects use
`open_read_only(path)`. Handle `HawdbError` using `Result` or the native
`raise` variants. Call `db.query(cypher, named_params)` or
`db.query_sql(sql, positional_params)` with the strongly typed `Value` ADT,
then inspect `QueryResult.columns`, `rows` or `get(row, column)`.
A low-level `query_raw` / `query_sql_raw` bridge remains for diagnostics
and JSON interoperability, not for ordinary application code.

MoonBit `Value::Int(Int64)` survives signed 64-bit boundaries without
floating-point roundtrip. The other variants are Bool, Null, Float, Text,
Binary, Uuid, Vector, List and Map. Note: UUID currently stores a textual
representation; the Rust adapter validates it when binding parameters.
The transport uses JSON with tagged binary, UUID and vector envelopes,
not an in-process borrowed Arrow/vector cursor. Materialized results
are owned and remain accessible after the database closes.

## Public API tiers (pre-1.0 review)

The recommended application-facing surface is deliberately small:

- **Core:** `open(path)`, `open_in_memory()`, `open_read_only(path)`,
  `Database::query` (Cypher named `Map[String, Value]` parameters),
  `Database::query_sql` (SQL positional `Array[Value]` parameters),
  `Database::close`, `Value`, `QueryResult` and `HawdbError`.
  Database calls return `Result[..., HawdbError]`; `get(row, column)`
  returns `None` if the row or column is absent, but `Some(Value::Null)`
  if the column exists and contains SQL/Cypher null.
- **Transactional core:** `begin`, `begin_read`,
  `Transaction::query` / `query_sql`, `commit`, `rollback`,
  `close` and the `with_transaction` / `with_read_transaction`
  scoped helpers. Never reuse a transaction after finish. If the caller
  explicitly closes the parent database, keeping a transaction value alive
  does not promise it can still execute.
- **Convenience:** `query_or_raise` and `query_sql_or_raise` adapt
  the same typed errors to MoonBit `raise`; they are not independent
  query engines.
- **Advanced/unstable:** `query_raw` and `query_sql_raw` expose
  JSON transport and string error envelopes. `HawdbParams`, `HawdbRow`,
  `to_params`, `from_row` and the external `#derive` source
  generator are opt-in record mapping APIs, not a built-in macro or ORM.

No existing public symbol has been removed in this consolidation pass.
This is an API-review classification, **not a stable-version promise**.
Transport JSON, generator syntax, transaction ownership and native linking
must be reviewed before claiming a production/public ABI commitment.
All recursive `Value::List` and `Value::Map` paths must respect the same
64-level codec bound; typed queries reject deeper parameter trees.

## Conformance

`tests/conformance/` exercises:
- Cypher graph traversal and typed named parameters
- SQL schema/positional parameter order
- vector schema and exact L2 distance
- Session-like persistent storage and reopen
- invalid query recovery, idempotent close, closed-handle rejection
- exact minimum/maximum signed Int64 values, Unicode and nesting
- binary bytes, UUID, vector, Null and nested Map/List ADTs

These are feature-contract tests, not certified TPC/LDBC/ANN benchmarks.
Write transactions use real HawDB engine begin/commit/rollback; pinned
read transactions provide stable snapshots. The MoonBit-first
`db.with_transaction(fn(tx) { ... })` helper commits on `Ok` and rolls
back on `Err`; `db.with_read_transaction` releases pinned snapshots.
Raw transaction handles also support explicit `begin`, `begin_read`,
`commit`, `rollback` and `close`. There are no simulated transactions.

Posoco SessionStore additionally needs a proper schema, uniqueness and
indexes, idempotent append/update contracts, versioned metadata, conflict
handling, crash/fault-injection tests and recovery policy; the existing
Session-like conformance test does **not** yet qualify a production adapter.

See `BUILDING.md` for frozen Cargo dependencies and the platform matrix.
Attribute-driven record generation is opt-in and deliberately restricted
to simple struct types; see `tools/README.md`.

## Repository layout

```
bindings/moonbit/
  moon.mod, moon.pkg
  hawdb.mbt                  Native ownership/open and raw transport
  value.mbt                  MoonBit ADT, typed codec, query/row API
  native/                    Independent Rust staticlib, direct engine dependency
  tests/conformance/         Executable cross-language behavior contracts
```

See `bindings/python/tests/` and `bindings/go/hawdb_test.go` for
the authoritative observable behavior baseline. MoonBit's API uses its own
ADT and error model, rather than porting host language class hierarchies.
