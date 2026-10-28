# Build, release and platform qualification

The MoonBit package is the first-class SDK, and `native/` is its own
Rust staticlib (direct HawDB engine dependency). Neither dependency nor
release is subordinate to `bindings/ffi`.

## Reproducibility

- Rust toolchain: `rust-toolchain.toml`, version 1.97.1.
- Standalone dependency graph: `native/Cargo.lock` committed in this branch.
- Production CI: `cargo build --release --locked` on Linux, macOS and Windows;
  Windows must link and run the same database conformance executable.
- MoonBit toolchain: public installer currently returns HTTP 403 for the
  historical binary path `0.1.20261023` even though that build passed Linux
  CI. For availability, CI installs the official latest channel and
  **fails closed unless the installed `moon` version is `0.1.20261023`**.
  A verified immutable binary URL/SHA256 is still required for release-grade
  supply-chain reproducibility.
- Generated code is checked in and verified with `--check`, plus
  negative-generator unit tests. Any divergence blocks CI.

## Platform matrix

| Host | Rust adapter | MoonBit API | Actual embedded execution |
| --- | --- | --- | --- |
| Ubuntu 24.04 (x86_64) | release staticlib | native | complete suite |
| macOS latest (Apple Silicon) | release staticlib | native | complete suite |
| Windows 2025 (x64) | release `.lib` staticlib | native | complete suite |

All three platforms completed actual native conformance on commit
`07860727fb6cb6c01b36ae2a122d1906d5972664`
([Actions #40659147687](https://github.com/colmugx/hawdb/actions/runs/40659147687)).
This is the last independently verified baseline before subsequent changes;
each later commit must qualify itself again.

These are platform qualifications, not performance comparisons.
Windows must also link the Rust staticlib into the MoonBit executable and
execute every CASE_PASS test; a typecheck or cargo check is insufficient.
Windows uses MSVC environment setup following colmugx/posoco's check.yml.

## Publishing

`moon.mod` advertises the HawDB MoonBit module with engine-compatible
version, but publishing should happen only after review of the API, test
matrix, toolchain pin, platform support policy and required licenses.
The native Rust adapter itself is intentionally `publish = false` (not a
standalone crates.io crate). Native consumers must build the matching Rust
library and link it with the MoonBit package; prebuilt artifacts/packaging
are not yet supplied.

## Scope limitations

The normal queries return owned `QueryResult` values. No Arrow/zero-copy,
retained cursor, public query planner controls or benchmark numbers are
implied. A scoped write transaction commits on successful callback and
rolls back on error; read scopes pin an engine read snapshot. Source
generation currently handles only simple annotated String/Int64/Bool
structs (see `tools/README.md`).

## Release staging (not registry publication)

After native conformance, all three platforms stage a per-OS/per-architecture tarball
and SHA256 checksum as a GitHub Actions artifact. The archive includes
MoonBit source, the matching Rust staticlib, package documentation, license,
and a provenance manifest containing the commit and tested toolchains.

To stage locally after building:
```sh
cd bindings/moonbit
python3 tools/stage_release.py --os Linux --arch X64
```

This is a **review artifact**, not an upstream release or a working
`moon install` distribution. Consumers still need to link the native static
library in their executable `moon.pkg`. Third-party attribution/SBOM,
stable native ABI policy, verified binary checksums and platform-native linker qualification must be reviewed before a public
production release.

On macOS, the full Rust static archive requires Apple frameworks
`CoreFoundation` and `IOKit`; CI adds these to the MoonBit executable's
`cc-link-flags` only on macOS. Applications linking the Rust staticlib must
supply equivalent platform-native system flags.

On Windows, CI initializes MSVC with `ilammy/msvc-dev-cmd@v1`, builds
`hawdb_moonbit.lib` using `cargo build --release --locked`, and uses
`tools/configure_native_link.py --os windows` to adapt the conformance
executable's platform-specific linking flags. This is modeled on Posoco's
3-OS testing strategy, extended to native database execution on every OS.
