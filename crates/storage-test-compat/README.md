# Storage Test Compatibility

This crate owns golden storage files used to verify DBSP storage format
compatibility across versions.

Golden files live in `crates/storage-test-compat/golden-files/` and are named:

- `golden-batch-v{VERSION}{COMPRESSION}-large-{FILTER}.feldera`
- `golden-batch-v{VERSION}{COMPRESSION}-small-{FILTER}.feldera`
- `golden-batch-v{VERSION}{COMPRESSION}-variant-{FILTER}.feldera`

where

- FILTER is one of "bloom", "roaring" and "modular".
- COMPRESSION is one of "" (empty) and "snappy"

The `large` files use the wide tuple format (`Tup65`) and the `small` files use
a compact `Tup8` format. Unit tests in this crate read these files and assert
roundtrip compatibility.

## Regenerating golden files

When the storage format version changes (see `dbsp::storage::file::format::VERSION_NUMBER`),
regenerate and commit new golden files:

```bash
cargo run -p storage-test-compat --bin golden-writer
```

This rewrites the `writer2-*` and `writer3-*` files in `golden-files/`.

## The `writer1-*-bloom` files are frozen

Those files hold single-module Bloom filters produced by a binary that predates
modular filters, and they are evidence that such a filter still loads and probes
correctly.

Do not regenerate them. Today's writer emits a multi-module filter at the
default false positive rate, so regenerating would replace the encoding under
test with the current one, and the compatibility check would pass while testing
nothing.

The golden writer skips them, and their names are pinned to
`LEGACY_BLOOM_VERSION` rather than to `VERSION_NUMBER`, so neither running the
writer nor bumping the format version touches them. Replacing them takes a
deliberate edit to the writer.
