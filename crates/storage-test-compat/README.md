# Storage Test Compatibility

This crate owns golden storage files used to verify DBSP storage format
compatibility across versions.

Golden files live in `crates/storage-test-compat/golden-files/` and are named:

- `golden-batch-v{VERSION}-large.feldera`
- `golden-batch-v{VERSION}-snappy-large.feldera`
- `golden-batch-v{VERSION}-small.feldera`
- `golden-batch-v{VERSION}-snappy-small.feldera`

The `large` files use the wide tuple format (`Tup65`) and the `small` files use
a compact `Tup8` format. Unit tests in this crate read these files and assert
roundtrip compatibility.

## Regenerating golden files

When the storage format version changes (see `dbsp::storage::file::format::VERSION_NUMBER`),
regenerate and commit new golden files:

```bash
cargo run -p storage-test-compat --bin golden-writer
```

This rewrites all `golden-batch-v{VERSION}*` files in `golden-files/`.
