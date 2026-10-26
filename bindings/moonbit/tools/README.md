# MoonBit type generation

Use `#derive.hawdb_params` or `#derive.hawdb_row` in a **source template**
(`.mbt.in`), not as a built-in MoonBit macro. The generator produces an
ordinary checked-in MoonBit source containing `HawdbParams` or `HawdbRow`
implementations.

```sh
python3 tools/hawdb_derive.py examples/records.mbt.in tests/conformance/records.generated.mbt
python3 tools/hawdb_derive.py examples/records.mbt.in tests/conformance/records.generated.mbt --check
```

The initial generator is intentionally limited to explicitly marked
`pub(all) struct` with `String`, `Int64` and `Bool` fields.
Unsupported types and syntax fail immediately. The generated source must
compile in CI. This narrow baseline will evolve toward MoonBit AST-driven
parsing after the parser is stable; do not imply it handles arbitrary MoonBit
syntax, Option types, generics, nested structs or field renames.

The public traits support handwritten codecs for all `Value` variants.
Generated decoders match by column name, not result column order, and report
missing/wrongly typed values instead of silently applying defaults.

The generated code references `@hawdb` imported from the SDK in the consumer package's `moon.pkg`; use `--alias other_alias` to choose a different import alias. Example records are isolated under `tests/conformance` and do not pollute the SDK's public namespace.
