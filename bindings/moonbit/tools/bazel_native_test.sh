#!/usr/bin/env bash
# Copyright 2026 Nowledge contributors
# SPDX-License-Identifier: Apache-2.0
#
# Execute real MoonBit Native programs using the Bazel-built adapter.
set -euo pipefail

mode="${1:?expected conformance or consumer}"
archive_runfile="${2:?expected Bazel staticlib runfile}"
runfiles="${TEST_SRCDIR:?}/${TEST_WORKSPACE:?}"
archive="${runfiles}/${archive_runfile}"
test -s "$archive"
command -v moon >/dev/null || { echo "MoonBit 'moon' is required on PATH" >&2; exit 1; }
command -v python3 >/dev/null || { echo "python3 is required on PATH" >&2; exit 1; }

workspace="${TEST_TMPDIR:?}/hawdb-moonbit-bazel-${mode}"
mkdir -p "$workspace/bindings"
# Dereference runfile symlinks so MoonBit may generate writable outputs without
# modifying the checkout or Bazel's immutable input tree.
cp -RL "$runfiles/bindings/moonbit" "$workspace/bindings/"
cp -RL "$runfiles/bindings/moonbit-consumer-smoke" "$workspace/bindings/"
sdk="$workspace/bindings/moonbit"
mkdir -p "$sdk/native/target/release"

case "$(uname -s)" in
  Linux)
    cp "$archive" "$sdk/native/target/release/libhawdb_moonbit.a"
    ;;
  Darwin)
    cp "$archive" "$sdk/native/target/release/libhawdb_moonbit.a"
    ;;
  *)
    echo "Bazel MoonBit executable test requires Linux/macOS; Windows has a separate native lane" >&2
    exit 1
    ;;
esac

cd "$sdk"
python3 tools/hawdb_derive.py examples/records.mbt.in tests/conformance/records.generated.mbt --check
python3 -m unittest discover -s tools/tests -p 'test_*.py'
python3 tools/check_api_surface.py
moon check --target native

if [[ "$(uname -s)" == Darwin ]]; then
  python3 tools/configure_native_link.py --os macos
fi

case "$mode" in
  conformance)
    rm -rf tests/conformance-db
    moon run --target native tests/conformance 2>&1 | tee conformance.log
    ! grep -q '^CASE_FAIL ' conformance.log
    grep -Fxq 'HAWDB_MOONBIT_BUSINESS_SUITE_PASS' conformance.log
    ;;
  consumer)
    if [[ "$(uname -s)" == Darwin ]]; then
      python3 tools/configure_native_link.py --os macos \
        --pkg ../moonbit-consumer-smoke/consumer/moon.pkg \
        --sdk-prefix ../../moonbit/
    fi
    cd "$workspace/bindings/moonbit-consumer-smoke/consumer"
    moon check --target native
    moon run --target native . 2>&1 | tee external-consumer.log
    ! grep -q '^EXTERNAL_CONSUMER_FAIL ' external-consumer.log
    grep -Fxq 'EXTERNAL_CONSUMER_PASS' external-consumer.log
    ;;
  *)
    echo "Unknown MoonBit Bazel test mode: $mode" >&2
    exit 2
    ;;
esac
