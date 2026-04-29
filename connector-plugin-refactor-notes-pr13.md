# PR 13 — Multi-tenant cache layout + supply-chain hardening docs: Discrepancies from Plan

### 1. Item 1 ("Confirm per-tenant cache directory layout is enforced") was already complete

The plan presents "Confirm per-tenant cache directory layout `/var/lib/feldera/describer/<tenant_id>/<content_hash>/` is enforced" as a new deliverable. In practice, `describer_workspace_dir` was already written to enforce this layout (PR 9/10), and `workspace_dir_keys_by_tenant_then_hash` already tested it. The only substantive work for item 1 was recognising that it was done and deciding not to duplicate the assertion. The plan should flag items that are already satisfied by prior PRs rather than listing them as open.

### 2. The HTTP-level isolation test cannot be written as a pure unit test

The plan says "Test that one tenant's `PUT /v0/connectors/connectors.toml` does not invalidate another tenant's manifest, lockfile, or build cache." This reads as an HTTP integration test, but the only available test harness for `connectors.rs` is synchronous unit tests without a running server. The isolation property is structural — it follows deterministically from `describer_workspace_dir` embedding the tenant UUID — so the useful test is a unit test asserting that the directory paths for two tenants are always disjoint. The plan should either scope the item to a unit test or note that it requires a multi-tenant integration fixture.

### 3. `edited_at` / `edited_by` audit headers are plan-ahead, not yet implemented

The plan lists "the in-product editor's audit log (`edited_at` / `edited_by`) as the review surface" as a hardening control. In the current codebase, `GET /v0/connectors/connectors.toml` does not return `edited_at` or `edited_by` headers — those fields are defined in the plan for a future PR (PR 15, in-product editor). Documenting them now in the hardening guide is forward-looking. The doc page describes the intended behavior but operators cannot use it yet. The plan should note which items in the hardening guide depend on future PRs.

### 4. `[patch]` validation is deliberately absent; the doc must say so explicitly

Feldera validates `connectors.toml` only at the line-shape level (`validate_connectors_toml_shape`), deliberately leaving Cargo as the authoritative parser. A `[patch]` section is valid line-shape but changes dependency resolution globally. There is no code guard preventing it. The hardening doc must explicitly state that `[patch]` is not blocked and explain why (Cargo is the parser, not Feldera), so operators do not assume enforcement exists. The plan's "auditing `[patch]`" item implies review rather than enforcement, but that distinction is easy to miss.
