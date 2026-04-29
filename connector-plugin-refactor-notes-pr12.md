# PR 12 — Build-Cache Discipline: Discrepancies from Plan

### 1. `prepare_workspace` needed `tenant_id` to locate the describer workspace

The plan says "copy `describer.lock` as `Cargo.lock` into every per-pipeline workspace (extends `prepare_workspace`)" without noting that `prepare_workspace` did not previously receive `tenant_id`. `describer_workspace_dir(config, tenant_id, cache_key)` requires the tenant ID to construct the path `<working_dir>/describer/<tenant_id>/<cache_key>/`. Adding `tenant_id: TenantId` to `prepare_workspace`'s signature was required; the plan should call this out explicitly.

### 2. `prepare_workspace` return type must change to signal `--locked` readiness

The plan treats "copy `describer.lock`" and "add `--locked` to `cargo build`" as two independent checklist items, but they are causally coupled: `--locked` is only safe when a fully-pinned lock was actually copied (full-warm path), and unsafe on the cold or partial-warm path where Cargo must still resolve dependencies. `call_compiler` needs to know which case applies. The cleanest interface is for `prepare_workspace` to return `bool` (previously `()`) and for `call_compiler` to accept a matching `use_locked: bool` parameter. The plan should model the `prepare_workspace → call_compiler` data flow explicitly rather than listing the two items as independent.

### 3. Partial-warm path for per-pipeline builds differs from the plan's "add --locked" framing

The plan says simply "add `--locked` to per-pipeline `cargo build` invocation." In practice, `--locked` cannot be added unconditionally: on the partial-warm path (describer.lock exists but its `describer.build_version` differs from the current Feldera version, e.g. after an upgrade) the lock still contains valid external-connector pins but outdated Feldera path-dep versions. Cargo cannot resolve with `--locked` on a partially-stale lock. `--locked` must be gated on `is_full_warm`, matching the describer's own three-tier logic. The plan's blanket "add --locked" formulation is correct only for the two always-fresh cases (full-warm describer lock and bundled platform lock).

### 4. Mold detection is per-build, not a "deployment config" knob

The plan says "optionally set `RUSTFLAGS="-C link-arg=-fuse-ld=mold"` in default deployment configs (gated on host having `mold` available)." "Deployment configs" implies Docker Compose / Helm chart edits. In practice, auto-detecting mold at build time (running `mold --version` per `cargo build` invocation) is safer than requiring operators to edit compose files: if mold is installed the flag is applied, if not it is silently omitted, and an explicit operator-supplied RUSTFLAGS always wins. The plan should either specify code-side detection (with `--version` probe) or document the exact compose-file variable to set — mixing "deployment configs" and "gated on host" is ambiguous.

### 5. RUSTFLAGS propagation constraint already existed; the plan item is documentation-only

The plan lists "document RUSTFLAGS constraint: must be set at manager level, not per pipeline" as a separate checklist item, implying new enforcement. The existing code already propagates `std::env::var_os("RUSTFLAGS")` from the manager process to `cargo build` and clears all other env vars. No new enforcement was needed — the constraint is architectural (the manager owns the subprocess environment). The deliverable for this item is a comment in `call_compiler`, not a code guard.
