#!/usr/bin/env bash
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail
if [[ -n "${TEST_SRCDIR:-}" && -n "${TEST_WORKSPACE:-}" ]]; then
  runner="${TEST_SRCDIR}/${TEST_WORKSPACE}/crates/search/tests/run-unit-family.sh"
else
  runner="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/run-unit-family.sh"
fi
fixture="$(mktemp -d "${TEST_TMPDIR:-${TMPDIR:-/tmp}}/hawdb-search-family-test.XXXXXX")"
trap 'rm -rf "${fixture}"' EXIT
export FAMILY_PROBE_ARGS="${fixture}/args"
cat >"${fixture}/binary" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == --list ]]; then
  if [[ "${FAMILY_PROBE_DISCOVERY_EXIT:-0}" != 0 ]]; then
    exit "${FAMILY_PROBE_DISCOVERY_EXIT}"
  fi
  echo 'other::case: test'
  if [[ "${FAMILY_PROBE_EMPTY:-0}" == 0 ]]; then
    echo 'waves::first: test'
    echo 'waves::second: test'
  fi
  if [[ "${FAMILY_PROBE_OVERLAP:-0}" == 1 ]]; then
    echo 'waves::other::case: test'
  fi
  exit 0
fi
printf '%s\n' "$@" >"${FAMILY_PROBE_ARGS}"
exit "${FAMILY_PROBE_EXIT:-0}"
EOF
chmod +x "${fixture}/binary"
"${runner}" "${fixture}/binary" waves:: --test-threads=1 caller_filter --exact
python3 - "${FAMILY_PROBE_ARGS}" <<'PY'
from pathlib import Path
import sys

args = Path(sys.argv[1]).read_text().splitlines()
assert args == ["--skip", "other::case", "--test-threads=1", "caller_filter", "--exact"]
PY
expect_exit() {
  local expected="$1"
  shift
  local observed=0
  "$@" >"${fixture}/error" 2>&1 || observed=$?
  if [[ "${observed}" != "${expected}" ]]; then
    cat "${fixture}/error" >&2
    echo "expected exit ${expected}, got ${observed}" >&2
    exit 1
  fi
}
expect_exit 42 env FAMILY_PROBE_EXIT=42 "${runner}" "${fixture}/binary" waves::
expect_exit 17 env FAMILY_PROBE_DISCOVERY_EXIT=17 "${runner}" "${fixture}/binary" waves::
rm -f "${FAMILY_PROBE_ARGS}"
expect_exit 1 env FAMILY_PROBE_EMPTY=1 "${runner}" "${fixture}/binary" waves::
[[ ! -e "${FAMILY_PROBE_ARGS}" ]]
expect_exit 1 env FAMILY_PROBE_OVERLAP=1 "${runner}" "${fixture}/binary" waves::
[[ ! -e "${FAMILY_PROBE_ARGS}" ]]
expect_exit 64 "${runner}"
echo "search unit family ownership, discovery, arguments and failure propagation passed"
