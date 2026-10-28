#!/usr/bin/env python3
"""Fail closed on unintended additions/removals to MoonBit public exports."""
from pathlib import Path
import re
import difflib

ROOT = Path(__file__).resolve().parents[1]
PUBLIC = re.compile(
    r"^\s*pub(?:\((?:all|open)\))?\s+"
    r"(?:(fn)(?:\[[^\]]+\])?\s+|(struct|enum|suberror|trait)\s+)"
    r"([A-Za-z_]\w*(?:::[A-Za-z_]\w*)?)",
    re.MULTILINE,
)

def surface():
    entries = []
    for path in sorted(ROOT.glob("*.mbt")):
        for match in PUBLIC.finditer(path.read_text(encoding="utf-8")):
            fn, kind, name = match.groups()
            entries.append(f"{fn or kind} {name}")
    if len(entries) != len(set(entries)):
        raise SystemExit("Duplicate public declarations")
    return "".join(x + "\n" for x in sorted(entries))

if __name__ == "__main__":
    actual = surface()
    expected = (ROOT / "public_api_surface.txt").read_text(encoding="utf-8")
    if expected != actual:
        print("Public API declaration drift: review API_CONTRACT.md and snapshot")
        print("".join(difflib.unified_diff(expected.splitlines(True),actual.splitlines(True))))
        raise SystemExit(1)
    print(f"PUBLIC_API_SURFACE_PASS {len(actual.splitlines())} declarations")
