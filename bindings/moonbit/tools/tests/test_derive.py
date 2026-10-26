"""Generator should fail closed, and emitted source must be deterministic."""
from __future__ import annotations

import importlib.util
import pathlib
import unittest

HERE = pathlib.Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("hawdb_derive", HERE / "hawdb_derive.py")
assert SPEC is not None and SPEC.loader is not None
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)


class DeriveContracts(unittest.TestCase):
    def test_known_fixture_is_deterministic(self) -> None:
        source = (HERE.parent / "examples" / "records.mbt.in").read_text(encoding="utf-8")
        expected = (HERE.parent / "tests" / "conformance" / "records.generated.mbt").read_text(encoding="utf-8")
        self.assertEqual(module.generate(source), expected)
        self.assertEqual(module.generate(source), module.generate(source))

    def test_reject_unknown_type(self) -> None:
        source = "#derive.hawdb_params\npub(all) struct Record {\n  score : Float32\n}"
        with self.assertRaisesRegex(ValueError, "Unsupported"):
            module.generate(source)

    def test_reject_ambiguous_input(self) -> None:
        source = "#derive.hawdb_row\npub(all) struct Record {\n  x : String\n}\nfn side_effect {}"
        with self.assertRaisesRegex(ValueError, "Unexpected syntax"):
            module.generate(source)

    def test_reject_duplicate_field(self) -> None:
        source = "#derive.hawdb_row\npub(all) struct Record {\n  x : String\n  x : String\n}"
        with self.assertRaisesRegex(ValueError, "duplicate"):
            module.generate(source)


if __name__ == "__main__":
    unittest.main()
