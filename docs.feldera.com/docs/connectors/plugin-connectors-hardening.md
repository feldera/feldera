# Hardening `connectors.toml`

`connectors.toml` extends Feldera with third-party connector crates by listing
them as Cargo dependencies. Because Cargo downloads and compiles that code on
the deployment host, every entry in the file is a **supply-chain trust
decision**. This page explains the controls available to operators who want to
reduce that risk.

## Pin git dependencies to a commit SHA, not a tag

Tags can be moved or deleted after the fact. A dependency declared as:

```toml
my_connector = { git = "https://github.com/acme/my-connector", tag = "v1.2.3" }
```

may resolve to a different commit tomorrow if the tag is force-pushed. Pin to
the exact commit SHA instead:

```toml
my_connector = { git = "https://github.com/acme/my-connector", rev = "a1b2c3d4e5f6..." }
```

Once `rev` is fixed, the only way to get a different commit is to change the
file — which shows up in the audit log (see [Audit trail](#audit-trail)).

## Vendor dependencies with `cargo vendor`

`cargo vendor` downloads every dependency into a local directory and rewrites
the `[source]` section of `Cargo.toml` to point at that snapshot. The result:

- No outbound network traffic during builds — connector crates cannot be silently
  replaced by upstream changes.
- All source code is available for code review before the first build.
- Works with private registries and air-gapped deployments.

Run it once in the describer workspace after the initial build, commit the
`vendor/` tree to your internal repository, and mount it into the Feldera
container. Set `CARGO_NET_OFFLINE=true` to prevent Cargo from reaching out
even if a `rev` resolves differently than expected.

## Review `[patch]` sections carefully

Cargo's `[patch]` table silently replaces any version of a crate across the
entire dependency graph. A `connectors.toml` fragment such as:

```toml
[patch.crates-io]
openssl = { git = "https://github.com/untrusted/openssl-fork" }
```

would substitute the `openssl` crate throughout **every** crate in the build,
including Feldera's own dependencies. Feldera validates the line-shape of
`connectors.toml` but does not parse or restrict `[patch]` semantics — that
analysis is left to the operator.

Treat any `[patch]` line with the same scrutiny you would apply to a
dependency on an unknown crate.

## Understand build-script privileges

Cargo runs `build.rs` files as part of compilation, on the same host and with
the same OS user as the Feldera pipeline-manager process. A crate whose
`build.rs` executes arbitrary shell commands can:

- Read files accessible to the pipeline-manager user.
- Make outbound network connections (unless the host firewall blocks them).
- Write to the build cache directory.

Before adding a crate with a non-trivial `build.rs`, review that script.
Prefer crates that are either build-script-free or whose build scripts are
short, audited, and stable across versions.

## Audit trail

Every write to `connectors.toml` through the Feldera API or the in-product
editor is recorded with:

- **`edited_at`** — UTC timestamp of the most recent change.
- **`edited_by`** — Identity of the principal who made the change.

Retrieve both fields from the `GET /v0/connectors/connectors.toml` response
headers. The in-product editor also displays them in the editor footer, giving
operators a quick at-a-glance check before accepting a build.

Make `edited_at` / `edited_by` part of your change-management workflow: audit
them after any failed or unexpected build, and integrate with your existing
logging pipeline by forwarding the Feldera API access logs.

## Summary checklist

| Control | Recommendation |
|---------|----------------|
| Git deps | Use `rev = "<full SHA>"` |
| Network isolation | Run `cargo vendor`; set `CARGO_NET_OFFLINE=true` |
| `[patch]` sections | Review every entry; prefer to disallow entirely |
| Build scripts | Audit `build.rs` before adding any crate that has one |
| Change review | Check `edited_at` / `edited_by` before approving builds |
