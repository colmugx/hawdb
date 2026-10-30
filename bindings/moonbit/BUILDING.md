# MoonBit SDK build and release evidence

## Rolling HawDB source, not frozen versions

The Rust adapter depends on HawDB at its **current repository checkout** via
`hawdb = { path = "../../.." }`. No HawDB version constraint is declared.
The adapter's `native/Cargo.lock` is generated **per build**, ignored by Git,
and not committed. CI calls `cargo test --release --lib` and
`cargo build --release`, without `--locked` or dependency version freezing.

The CI uses Rust stable and the current official MoonBit toolchain. Their
actual versions, the actual source commit, and the generated dependency
resolution are recorded as review-artifact provenance.

The CI path filters cover `src/**`, `crates/**`, root `Cargo.toml` and
`Cargo.lock`, as well as the SDK, so changes in current HawDB source rerun
the full compatibility suite. Future upstream commits may introduce breaking
behavior: CI must reveal this, not mask it with an old lockfile.

The MoonBit `moon.mod` semver is package identity, **not** a dependency
pin on HawDB. The external consumer's `moon.work` takes the SDK from local
source; its declared `@version` is ignored for workspace-local modules.

## Bazel test ownership

The existing **Native Bindings** GitHub Actions workflow now executes the
MoonBit Rust adapter, full embedded-database conformance and independent
consumer as Bazel tests on Linux/macOS alongside Go and C FFI. With the
current MoonBit compiler on `PATH`, run:

```sh
bazel build //bindings/moonbit/native:hawdb_moonbit_static
bazel test --test_output=errors --test_env=PATH --test_env=HOME \
  //bindings/moonbit:hawdb_moonbit_tests
```

The independent module acceptance fixture lives under
`tests/external-consumer/`, together with its own `moon.work` and
`moon.mod`; it is a test fixture, not a second binding package.
The Bazel test runner stages the static library from Bazel into a writable
MoonBit workspace; it never invokes Cargo or rebuilds the HawDB engine.
The legacy Native SDK workflow still owns Windows tests and review artifact
staging; its duplicate Linux Bazel pilot and temporary GC logging were
removed. Windows must retain its real executable/consumer coverage until
equivalent Bazel Windows tests pass.

## Native platform qualification

| Host | Adapter | Tests |
| --- | --- | --- |
| Ubuntu 24.04 x64 | release staticlib | Rust lifecycle, MoonBit engine conformance, separate consumer |
| macOS ARM64 | release staticlib | Rust lifecycle, MoonBit engine conformance, separate consumer |
| Windows 2025 x64 | release staticlib | Rust lifecycle, MoonBit engine conformance, separate consumer |

Read-only open selects HawDB's `RecoveryMode::Strict` rather than
`AutoRepairTornTail`. Conformance reopens the already-persisted session
without a writable recovery or checkpoint, verifies its contents, and
confirms read-only mutation refusal. A genuinely corrupt/torn WAL must
still fail closed in strict mode.

Build in `bindings/moonbit/native/`:

```sh
cargo test --release --lib
cargo build --release
```

Then, in `bindings/moonbit/`:

```sh
moon check --target native
moon run --target native tests/conformance
python3 tools/stage_release.py --os Linux --arch X64
```

On macOS and Windows, configure native linker flags with
`tools/configure_native_link.py` (as in the workflow).

## Review archive versus publication

After full conformance, each runner stages a tarball with MoonBit source,
Rust static library, API contract, license, native dependency resolution
generated during the build, SHA-256 inventory and toolchain/source provenance.
The graph captured in an artifact documents **that build**; it never pins
future SDK builds. Rolling builds are not reproducible across time.

These review artifacts are not published releases. Third-party attribution,
GC/finalizer race qualification, verified toolchain distribution, signature
policy and production release approval are separate gates.
