# MoonBit SDK API contract (pre-1.0)

This document is a **source-verified interface inventory and behavior contract**,
not a published ABI guarantee. The module is `nowledge-co/hawdb`, native-only.
The embedded engine is accessed through the dedicated Rust staticlib; there is
no `.c` adapter, no dependency on generic `bindings/ffi`, and no server.

## Public interface: preferred

| Group | Exports | Contract |
| --- | --- | --- |
| Open/close | `open(path)`, `open_read_only(path)`, `open_in_memory()`, `Database::close()`, `Database::is_open()` | Open returns `Result[Database,HawdbError]`; close is idempotent |
| Queries | `Database::query(statement, Map[String,Value])`, `Database::query_sql(statement, Array[Value])` | Named Cypher and positional SQL; typed `Result[QueryResult,HawdbError]` |
| Values | `Value::{Null,Bool,Int,Float,Text,Binary,Uuid,Vector,List,Map}` | `Int` is signed `Int64`; binary and vector are not strings |
| Results | `QueryResult.columns`, `QueryResult.rows`, `QueryResult::get(row,column)` | Materialized results, owned after close; `get` returns `None` for absent row/column but `Some(Value::Null)` for present null |
| Errors | `HawdbError::{Engine(kind,message),Codec(message),Closed}`, `HawdbError::describe()` | Engine kind is an extensible string, not a closed enum |
| Transactions | `Database::begin()`, `begin_read()`, `Transaction::query()`, `query_sql()`, `commit()`, `rollback()`, `close()`, `is_open()` | One write transaction per handle; no nested write; explicit close rolls back unfinished writes |
| Scoped | `Database::with_transaction(body)`, `with_read_transaction(body)` | `body : (Transaction)->Result[T,HawdbError]`; `Ok` commits, `Err` rolls back; cleanup is deterministic |
| Diagnostics | `version()` | Native adapter version string, not an ABI handshake |

## Secondary interfaces

- `Database::query_or_raise()` and `query_sql_or_raise()` are exact
  `Result`-to-`raise HawdbError` adapters; use them in code that expects
  MoonBit's error propagation. They do **not** alter query semantics.
- `Database::query_raw()`, `query_sql_raw()` return
  `Result[String,String]` with transport-level JSON and raw error envelope.
  This interface is for diagnostics/interoperability and is not frozen.
- `HawdbParams`, `HawdbRow`, `to_params()`, `from_row()` support
  application-level record mapping. The separate `hawdb_derive.py` is a
  restricted deterministic source generator, **not** a built-in MoonBit macro.

## Error and lifecycle invariants

1. A closed Database or Transaction yields `HawdbError::Closed` for typed
   queries, even if supplied parameters are invalid. A failed statement on an
   open handle must leave the handle usable unless the engine aborts its scope.
2. `Codec` means that input/result transport representation cannot be
   encoded or decoded (including >64 nested Value levels, invalid tagged
   representations, and duplicate or malformed result columns).
   Invalid data rejected by the engine's parameter binder may instead return
   `Engine("invalid_argument", ...)`.
3. `HawdbError::Engine.kind` and message are **not** a stable exhaustive set
   of error codes. Do not branch on human-readable messages.
4. The user calls `close()` explicitly; GC finalization is a fallback.
   `Database::close()` invalidates outstanding Transactions; subsequent
   query/commit is `Closed`, and the Native owner retains the engine until
   rollback/read-snapshot cleanup. Finished transactions must not be reused.
5. Scoped transaction callbacks return `Result`; they are not promised
   to accept a callback with an additional `raise` effect. MoonBit `defer`
   also closes on an abnormal exit. A failed rollback may replace the body's
   original error, as the existing API does.
6. Database and Transaction objects are **not offered as shareable across
   threads**. All FFI pointers, finalizer scheduling, simultaneous close/query
   races and reentrancy still require dedicated stress qualification.

## Data/encoding boundaries

- Both database and transaction paths share the same private parameter codec.
  Signed Int64 is encoded without conversion via Double.
- A singleton Map with a reserved `$binary`, `$uuid`, `$vector` or
  `$map` key is escaped to preserve Map identity. Those tagged envelopes
  are an internal protocol coupled to a matching MoonBit/Rust build.
- Values have a 64-level recursive depth budget. Non-finite floating values
  and empty vectors are rejected. Rust checks that a vector coordinate
  remains finite when converted to engine `f32`.
- `Uuid(String)` is text in MoonBit but checked by Rust on bind.

## Not covered by this API review

Public registry publication, immutable MoonBit binary pinning, ABI versioning,
prebuilt native linker integration, SBOM/licensing completeness, cross-thread
race safety, crash/fault injection, and Posoco SessionStore atomic cursor/retry
semantics. They are separate release/production acceptance gates.

## CI

See `tests/conformance/main.mbt` (real engine) and
`.github/workflows/moonbit-native.yml` (Linux, macOS, Windows).
No test may be deleted to make a build green.

## API drift guard and independent consumer

The committed `public_api_surface.txt` is the reviewed export list; the
`tools/check_api_surface.py` guard prevents accidental changes to public
declaration names/kinds. The separate module in
`bindings/moonbit-consumer-smoke/` is built and executed on all three native
CI hosts against an unpublished local workspace dependency. Its success marker
is `EXTERNAL_CONSUMER_PASS`. This verifies local-source consumption, not a
registry install, prebuilt binary distribution or cross-thread safety.

## Native lifecycle hardening candidate

The Rust adapter now serializes Handle/TransactionHandle mutable state using
per-object Mutex guards and retains engine ownership with Arc. This removes
unprotected mutation of raw error and database pointers. Calls use MoonBit
borrowed foreign arguments; this does not extend the validity of a payload
if native callers manually violate the MoonBit reference-lifetime contract.
Native cross-thread use is still not part of the supported MoonBit API until
stress/sanitizer qualification explicitly passes. This change is not a
public ABI version or release approval.
