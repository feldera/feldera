# PR 8 — Implementation Notes

Notes on unexpected factors encountered during implementation of PR 8
("Open the `TransportConfig` enum", Phase 7).

---

## 1. `#[serde(tag, content)]` is a derive-macro helper attribute — cannot survive derive removal

**Plan assumption**: Retain `#[serde(tag = "name", content = "config", rename_all = "snake_case")]` on the enum for `ToSchema` only, while implementing `Serialize`/`Deserialize` manually.

**Reality**: The `#[serde(...)]` outer attribute is a *helper attribute* injected by the `serde` derive macro proc-macro. Without `#[derive(Serialize)]` or `#[derive(Deserialize)]` present, the Rust compiler rejects it:

```
error: cannot find attribute `serde` in this scope
  --> config.rs:1634:3
   |
   | #[serde(tag = "name", content = "config", rename_all = "snake_case")]
   |   ^^^^^
```

**Fix applied**: Remove the attribute entirely.

**Side effect**: `utoipa`'s `ToSchema` derive previously read these serde attributes to generate an adjacently-tagged JSON schema (`{"type": "object", "required": ["name"], ...}`). Without the attribute, ToSchema falls back to its default enum schema (likely `oneOf` with inline struct schemas per variant). The OpenAPI schema for `TransportConfig` is now less precise. This is acceptable as a placeholder — Phase 9 / PR 14 is the designated owner of the correct OpenAPI schema for connector configs.

**Plan fix needed**: Add a note to PR 8 checklist: *"Remove `#[serde(tag, content, rename_all)]` from the enum definition — it cannot coexist with manual Serialize/Deserialize. Document the ToSchema schema regression as a known limitation until PR 14."*

---

## 2. `serde_json::Value` implements `Eq` — no derive change needed

**Potential concern mentioned in plan (implicitly)**: Adding `Plugin(PluginTransportConfig)` with `config: serde_json::Value` might break the `Eq` derive on `TransportConfig` (since JSON floats don't have a natural `Eq`).

**Reality**: `serde_json::Value` *does* implement `Eq`. `serde_json::Number` stores floats as bit patterns (`u64` via an internal enum), so `Eq` can be derived without any "NaN != NaN" violation. No changes to the `Eq` derive on `TransportConfig` or `PluginTransportConfig` were needed.

**Plan impact**: None — but worth noting because the assumption that `serde_json::Value: Eq` is non-obvious and not documented anywhere in the plan.

---

## 3. Manual Deserialize uses `serde_json::Value` as intermediate — slightly less efficient than derive

The derive-generated tagged deserializer streams directly from the input into each typed config. The manual impl parses the `"config"` field into a `serde_json::Value` first, then passes it to `serde_json::from_value::<TypedConfig>()`. This allocates an intermediate `Value` tree for every deserialization.

**Impact**: Minimal in practice — `TransportConfig` is deserialized once at pipeline startup. No hot-path concern.

**If ever a concern**: Replace the intermediate `Envelope.config: JsonValue` with `Envelope.config: Box<serde_json::value::RawValue>` and use `serde_json::from_str` / `serde_json::from_raw_value` at each arm. Avoids the allocation but is harder to read.

---

## 4. `HttpOutput` unit-variant serialization: no "config" field must be emitted

The prior derived `Serialize` for `HttpOutput` (a unit variant under `#[serde(tag, content)]`) emitted `{"name": "http_output"}` without a `"config"` key. The manual `Serialize` must match this exactly for stored pipeline configurations to deserialize identically after the upgrade.

This required a special `HttpOutput` arm that uses `serialize_map(Some(1))` (one entry only), confirmed by test `http_output_unit_variant_no_config_field`. Forgetting the `Some(1)` vs `Some(2)` distinction would have silently added a spurious null `"config"` field.

**Plan fix needed**: Add explicit note that the `HttpOutput` unit variant serializes with exactly one map entry (no "config" key). The PR 8 checklist item "Verify a stored pipeline configuration from before this PR deserializes identically" should specifically call out `HttpOutput`.

---

## 5. Exhaustive match audit: only `program.rs` needed explicit Plugin arms

**Plan prediction**: "the reviewer should expect this PR to touch ~5 files outside `config.rs`".

**Reality**: Adding `Plugin` to `TransportConfig` triggered zero compile errors outside `config.rs` and `program.rs`. All other files that reference `TransportConfig` variants use either:
- `_` wildcards (`format/csv.rs`, `format/parquet.rs`, `format/avro/output.rs`, `format/json/output.rs`, `transport/s3.rs`)  
- `if let` / `matches!` (non-exhaustive patterns)
- Struct construction (not matching)

The two validation matches in `program.rs` at lines 682 and 735 already had `_ =>` catch arms, so they didn't produce compile errors either — but they needed *explicit* `Plugin` arms to return the semantically correct `UnknownConnector` error instead of the misleading `ExpectedInputConnector`/`ExpectedOutputConnector`.

**Plan fix needed**: The "~5 files" prediction is wrong for this branch state (post PR 7g). The plan should say "most matches use `_` wildcards and require no code change; only `program.rs` needs explicit Plugin arms for correct error semantics." This helps reviewers set the right scope expectation.

---

## 6. `new_from_connector_generation_error` is an exhaustive match over `ConnectorGenerationError`

The function at `program.rs:127` matches exhaustively over `ConnectorGenerationError` without a `_` arm. Adding `UnknownConnector` to the error enum required a corresponding branch here. Easy to miss if only looking at the TransportConfig match sites; the compiler enforces it.

**Plan fix needed**: Add to the PR 8 checklist: *"Update `new_from_connector_generation_error` to handle `UnknownConnector { position, .. }`."*

---

## 7. Plugin connector validation blocks end-to-end plugin use until PR 11

The `Plugin` arm in both input and output validation matches in `program.rs` returns `UnknownConnector` error. This means **any pipeline SQL that references a plugin connector fails at SQL compilation** until PR 11 wires manifest-based direction validation.

This is correct per the plan ("reject the config... rather than panic with `unreachable!()`"), but the practical implication is that PR 8 alone does not enable any end-to-end plugin use. Plugin connectors can be stored and deserialized (no more JSON parse error on unknown names), but they cannot be wired into a SQL relation until PR 11.

**Plan fix needed**: Add to the PR 8 description: *"After this PR, `TransportConfig::Plugin` configs round-trip through serde without error, but the pipeline compiler still rejects them with `UnknownConnector`. End-to-end plugin use requires PR 11."* This sets correct expectations for anyone testing the feature incrementally.

---

## 8. `is_transient()` and `is_http_input()` had zero callers at removal time

Both methods were removed in this PR as planned. Confirmed via `rg "is_transient\|is_http_input" --type rust crates/` returning zero results. The PR 5 controller-rewrites had already replaced all call sites with descriptor lookups. No surprises here.

---

## 9. `transport.rs` and `integrated.rs` are fully registry-dispatched — zero impact from `Plugin`

After PRs 7a–7g, both factory functions dispatch entirely through the connector descriptor registry. There are no exhaustive `match TransportConfig { ... }` statements in either file. Adding `Plugin` caused no compile errors and no behavioral changes in the dispatch path: `connector_by_name(&config.name())` will return `None` for any unregistered plugin name, and the function returns `Ok(None)` or `Err(unknown_*_transport(...))` as before.

---

## Summary of plan updates needed

| # | Plan location | Update |
|---|---|---|
| 1 | PR 8 checklist | Remove `#[serde(tag, content, rename_all)]` from enum; note ToSchema regression until PR 14 |
| 1 | PR 8 checklist | *Remove* "the `#[serde(tag, content)]` attribute is retained for `ToSchema` only" — it can't be retained |
| 4 | PR 8 checklist | Add: verify `HttpOutput` unit-variant round-trip (no spurious null "config" field) |
| 5 | PR 8 description | Change "~5 files outside config.rs" to "only `program.rs` needs explicit Plugin arms" |
| 6 | PR 8 checklist | Add: update `new_from_connector_generation_error` match |
| 7 | PR 8 description | Add: plugin connectors remain blocked until PR 11; PR 8 only enables graceful deserialization |
