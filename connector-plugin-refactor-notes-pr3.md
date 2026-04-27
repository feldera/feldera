# Connector Plugin Refactor — PR 3 Notes

## What was done

Phase 3: Replaced the manual `Lazy<BTreeMap>` format registries with `inventory`-based open registries.

### Files changed

| File | Change |
|------|--------|
| `crates/adapterlib/src/format.rs` | Added `inventory::collect!(&'static dyn InputFormat)` and `inventory::collect!(&'static dyn OutputFormat)` after the respective trait definitions; added `pub fn get_input_format` and `pub fn get_output_format` discovery functions; added 6 unit tests for the registry. |
| `crates/adapterlib/src/lib.rs` | Added `register_input_format!` and `register_output_format!` macros (parallel to `register_connector!`). |
| `crates/adapters/src/format.rs` | Removed `once_cell::sync::Lazy`, `std::collections::BTreeMap`, all format-type imports, `INPUT_FORMATS` / `OUTPUT_FORMATS` statics, `get_input_format` / `get_output_format` functions, and the TODO comment. |
| `crates/adapters/src/format/csv.rs` | `inventory::submit! { &CsvInputFormat as &dyn InputFormat }` and `inventory::submit! { &CsvOutputFormat as &dyn OutputFormat }`. |
| `crates/adapters/src/format/json/input.rs` | `inventory::submit! { &JsonInputFormat as &dyn InputFormat }`. |
| `crates/adapters/src/format/json/output.rs` | `inventory::submit! { &JsonOutputFormat as &dyn OutputFormat }`. |
| `crates/adapters/src/format/json.rs` | Removed the `pub use input::JsonInputFormat` and `pub use output::JsonOutputFormat` re-exports (they existed solely to support the old BTreeMap factory; the types are still accessible within their own modules). |
| `crates/adapters/src/format/parquet.rs` | `inventory::submit!` for both `ParquetInputFormat` and `ParquetOutputFormat`. |
| `crates/adapters/src/format/avro/input.rs` | `inventory::submit! { &AvroInputFormat as &dyn InputFormat }` (compiled only when `with-avro` feature is active — the entire `avro` module is `#[cfg(feature = "with-avro")]`). |
| `crates/adapters/src/format/avro/output.rs` | `inventory::submit! { &AvroOutputFormat as &dyn OutputFormat }`. |
| `crates/adapters/src/format/raw.rs` | `inventory::submit! { &RawInputFormat as &dyn InputFormat }`. |

### Where `inventory::collect!` lives

The plan mentioned placing the slots "in `crates/adapters/src/format.rs`" (literally replacing the `Lazy` statics there). However, the slots were instead placed in `crates/adapterlib/src/format.rs`, alongside the trait definitions. This is the correct choice:

- External format crates can only depend on `feldera-adapterlib`, not `dbsp_adapters`.
- `inventory::collect!` in adapterlib means any linked crate (including external ones) can submit to it.
- The `get_input_format` / `get_output_format` discovery functions, also in adapterlib, are re-exported to `adapters` via the existing `pub use feldera_adapterlib::format::*` in `adapters/src/format.rs`. All existing call sites in `adapters` (`controller.rs`, `server.rs`, `test.rs`, etc.) required no changes.

This diverges from the literal plan wording but is the only placement that actually enables third-party format registration — which is the stated goal of Phase 3.

### Macro design decision

The plan called for `register_input_format!` / `register_output_format!` macros in adapterlib. Two approaches were considered:

1. **Cast inside macro**: `inventory::submit! { $factory as &dyn $crate::format::InputFormat }` — This would coerce automatically, but `$crate` inside a nested macro invocation can be tricky and adding a type cast inside `inventory::submit!` is not standard usage.

2. **Pass-through wrapper** (chosen): The macros simply forward to `inventory::submit!` with no cast. Callers provide the full typed expression: `register_input_format!(&MyFormat as &dyn feldera_adapterlib::format::InputFormat)`. This matches how the storage crate uses `inventory::submit!` directly (`&DefaultBackendFactory as &dyn StorageBackendFactory`).

In-tree format modules in `adapters` skip the adapterlib macros and call `inventory::submit!` directly (since `inventory` is already a direct dep of `adapters`) — this is the simpler pattern for in-tree code. The macros are intended for external crates.

### Const-evaluability of `&UnitStruct as &dyn Trait`

All five built-in format types (`CsvInputFormat`, `JsonInputFormat`, `JsonOutputFormat`, `ParquetInputFormat`, `ParquetOutputFormat`, `AvroInputFormat`, `AvroOutputFormat`, `RawInputFormat`) are zero-sized unit structs. The expression `&CsvInputFormat as &dyn InputFormat` is const-evaluable in current Rust (fat-pointer vtable coercions of ZSTs are const-stable). This is confirmed by the existing `storage/backend/posixio_impl.rs` which uses the same pattern.

If a future format is NOT a unit struct (i.e., has fields and can't be expressed as a const reference), the author must declare a `static`:

```rust
static MY_FORMAT: MyFormat = MyFormat { field: value };
inventory::submit! { &MY_FORMAT as &dyn InputFormat }
```

This constraint is the same as for `ConnectorDescriptor` (PR 2) and should be documented.

### Feature-gated formats (avro)

The entire `avro` module in `adapters` is behind `#[cfg(feature = "with-avro")]` in `format.rs`:

```rust
#[cfg(feature = "with-avro")]
pub mod avro;
```

This means the `inventory::submit!` calls inside `avro/input.rs` and `avro/output.rs` are only compiled when the feature is active — no extra `#[cfg]` gates needed on the submit calls themselves. The original BTreeMap code used explicit `#[cfg(feature = "with-avro")]` guards on individual map entries; the inventory approach is cleaner because the gate sits at the module boundary.

### `json.rs` re-export cleanup

The `pub use input::JsonInputFormat` and `pub use output::JsonOutputFormat` in `json.rs` were dead code after the BTreeMap was removed. The `json` module is private (`mod json;` in `format.rs`, not `pub mod`), so these re-exports were only reachable within `dbsp_adapters`. The Rust compiler correctly flagged them as "unused imports". Removing them is a pure cleanup.

The analogy: `csv.rs`, `parquet.rs`, and `raw.rs` don't re-export their format types at the module level either — their types are visible within `dbsp_adapters` via `use crate::format::csv::CsvInputFormat` but are not re-exported to the crate root. The json module now behaves consistently.

### `raw` has no `OutputFormat`

`RawInputFormat` only implements `InputFormat` (there is no `RawOutputFormat`). The original BTreeMap reflected this: `OUTPUT_FORMATS` had no "raw" entry. The inventory approach naturally handles this — only one `submit!` call in `raw.rs`.

---

## Insights and factors not accounted for in the plan

### 1. `inventory::collect!` placement contradicts the plan's literal wording

The plan says "Replace the `Lazy<BTreeMap>` pattern at lines 30 and 49" (i.e., in `adapters/src/format.rs`). The correct placement is `adapterlib/src/format.rs`. The plan's wording conflated "where the old code lives" with "where the new code should live." The motivation (enabling external crates) unambiguously points to adapterlib. **Update the plan** to say: "`inventory::collect!` is added to `adapterlib/src/format.rs` alongside the trait definitions; `adapters/src/format.rs` removes the old statics entirely."

### 2. `get_input_format` / `get_output_format` move to adapterlib implicitly

The plan did not explicitly say these discovery functions move to adapterlib — it only said "update `get_input_format()`/`get_output_format()` to walk the inventory." Because `inventory::collect!` is in adapterlib, the functions must be there too (you can only iterate a collection from the crate that declared it, or any crate linked against it that has the `inventory::iter` call). The move happens automatically and all existing call sites (in `adapters`) pick them up via the existing `pub use feldera_adapterlib::format::*` wildcard re-export. **No call sites needed updating** — this is a clean zero-diff migration from adapters' perspective.

### 3. The `register_input_format!` / `register_output_format!` macros are optional for in-tree code

The macros exist for external crates (which can't use `inventory::submit!` directly without knowing the collected type). In-tree format modules in `adapters` call `inventory::submit!` directly. This is simpler and consistent with how the storage backend submits itself. The plan's checklist said to ship these macros; they are shipped and work for external use, but in-tree code uses the direct approach.

### 4. The `$crate` path-in-nested-macro approach is not viable for the cast form

An earlier draft of the macros tried `inventory::submit! { $factory as &dyn $crate::format::InputFormat }` to auto-coerce the argument. This was dropped because:
- `$crate` inside a nested macro invocation becomes a hygiene issue — the `inventory::submit!` macro sees the literal tokens `$crate::format::InputFormat`, which won't expand correctly in all contexts.
- The storage crate's existing pattern (`&DefaultBackendFactory as &dyn StorageBackendFactory` passed directly to `inventory::submit!`) is simpler and proven.

The final macros are simple pass-throughs that require callers to write the cast explicitly. This is slightly more verbose but avoids any hygiene surprises.

### 5. `inventory` requires `Sync` on the collected type — already satisfied

`inventory::collect!(&'static dyn InputFormat)` requires `&'static dyn InputFormat: Sync`. Since `InputFormat: Send + Sync` is stated in the trait bound (`pub trait InputFormat: Send + Sync`), `dyn InputFormat` is `Send + Sync`, and therefore `&'static dyn InputFormat` is `Sync`. No extra bounds needed.

### 6. No `unsafe impl Sync` needed for the format factories

The connector descriptor (PR 2) required `unsafe impl Sync for ConnectorDescriptor` because `ConnectorDescriptor` is a struct with function-pointer fields that the compiler can't automatically verify are `Sync`. For `&'static dyn InputFormat`, the trait bound already declares `Sync`, so no `unsafe impl` is needed on the factory types. The unit structs `CsvInputFormat`, etc., are trivially `Sync`.

### 7. `once_cell` dep removal in `adapters/src/format.rs`

Removing `use once_cell::sync::Lazy` from `adapters/src/format.rs` was safe because `once_cell` is used elsewhere in `adapters` (e.g., `transport/` modules). No `Cargo.toml` change needed.

### 8. Test placement: adapterlib tests work for the registry, but format-specific tests remain in adapters

The 6 new tests in `adapterlib/src/format.rs` use stub format types and test the registry mechanics (iterate, look up by name, miss on unknown). The built-in format names ("csv", "json", etc.) are only known at link time when `dbsp_adapters` is linked — they are NOT present when running `cargo test -p feldera-adapterlib` alone. This is expected and correct: adapterlib tests cover the registry contract; adapters tests cover the actual format submissions.

A follow-up could add a test in `adapters/src/format.rs` that verifies all expected built-in names are present in the registry. This is deferred but easy to add.

### 9. The "raw" format is input-only — inventory naturally handles asymmetry

The original `OUTPUT_FORMATS` BTreeMap simply didn't include "raw". The inventory approach handles this equally naturally — there is no `inventory::submit!` for output in `raw.rs`. No special logic needed.

### 10. Phase 3 resolves coupling site #6 from the plan

The plan's "Coupling sites to remove" table listed:
> `#6` | Format registries | `crates/adapters/src/format.rs:30,49` | `Lazy<BTreeMap>` with TODO for runtime registration

This coupling site is now fully resolved. The TODO comment is removed.

---

## Plan updates to carry forward

1. **Clarify PR 3 `inventory::collect!` placement**: In the plan, change "Replace the `Lazy<BTreeMap>` pattern at lines 30 and 49 with `inventory::collect!(…)` slots" to: "Add `inventory::collect!` slots and discovery functions to `adapterlib/src/format.rs`; remove the `Lazy<BTreeMap>` statics and old discovery functions from `adapters/src/format.rs`."

2. **Document const-evaluability constraint for format factories**: Add a note (analogous to the one for `ConnectorDescriptor`) that `inventory::submit!` requires a const-evaluable expression. Unit structs work directly; structs with fields require a named `static`.

3. **Note that `register_input_format!` / `register_output_format!` are for external crates**: In-tree format modules call `inventory::submit!` directly. External crates use the macros (which are pass-throughs). This distinction should be clear in the developer guide (Phase 11).

4. **Test gap**: No test in `adapters` verifies that the built-in format names are present. Add one (either in `adapters/src/format.rs` or as a dedicated test file) to guard against regression where a submit call is accidentally removed.
