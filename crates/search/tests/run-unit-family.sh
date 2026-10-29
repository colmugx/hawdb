#!/usr/bin/env bash
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail
if [[ $# -lt 2 ]]; then
  echo "usage: $0 <unit-test-binary> <family-prefix> [libtest arguments]" >&2
  exit 64
fi
unit_binary="$1"
family_prefix="$2"
shift 2
inventory="$(mktemp "${TEST_TMPDIR:-${TMPDIR:-/tmp}}/hawdb-search-family.XXXXXX")"
trap 'rm -f "${inventory}"' EXIT
"${unit_binary}" --list >"${inventory}"
arguments=("${unit_binary}")
owned_names=()
skipped_names=()
while IFS= read -r entry; do
  case "${entry}" in
    *": test") case_name="${entry%: test}" ;;
    *) continue ;;
  esac
  if [[ "${case_name}" == "${family_prefix}"* ]]; then
    owned_names+=("${case_name}")
  else
    arguments+=(--skip "${case_name}")
    skipped_names+=("${case_name}")
  fi
done <"${inventory}"
if [[ ${#owned_names[@]} -eq 0 ]]; then
  echo "search unit family has no discovered cases: ${family_prefix}" >&2
  exit 1
fi
if [[ ${#skipped_names[@]} -gt 0 ]]; then
  for owned_name in "${owned_names[@]}"; do
    for skipped_name in "${skipped_names[@]}"; do
      if [[ "${owned_name}" == *"${skipped_name}"* ]]; then
        echo "search unit family name overlap: ${owned_name} contains ${skipped_name}" >&2
        exit 1
      fi
    done
  done
fi
rm -f "${inventory}"
trap - EXIT
exec "${arguments[@]}" "$@"
