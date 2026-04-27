# PR 7e Notes — nexmark (input-only generator)

## Unexpected findings

### 1. Nexmark was the last constructing arm in the input fallback — input match collapses like output

The plan implied PR 7g would do the final input-match cleanup, but after nexmark migrates there are zero constructing arms remaining. The `let endpoint: Box<dyn TransportInputEndpoint> = match config { ... }; Ok(Some(endpoint))` structure would have had unreachable code at `Ok(Some(endpoint))` (the match has type `!` since all arms diverged via `return`). Rather than add `#[allow(unreachable_code)]`, the input fallback was collapsed to `match config { _ => Ok(None) }` — matching the output fallback state reached in PR 7d. PR 7g's input-fallback cleanup work is now pre-empted; only the `mod nexmark;` declaration and the wildcard arm remain to strip.

### 2. `ConnectorKind::Regular`, not `Transient`, despite the plan calling it "transient/generator"

The plan labels nexmark "transient/generator" but `ConnectorKind::Transient` means "not re-created when the pipeline restarts from a checkpoint" (HTTP direct, Clock, AdHoc). Nexmark implements `fault_tolerance() → Some(FtModel::ExactlyOnce)` and participates in the checkpoint/replay cycle, so it is `ConnectorKind::Regular` — separate transport + format layers with normal lifetime semantics.

### 3. Descriptor block lives inside the `#[cfg(feature = "with-nexmark")]` module

Unlike redis (a directory sub-module) or kafka (a wrapper module with a `mod.rs`-style file), nexmark is a single `nexmark.rs` file gated by `#[cfg(feature = "with-nexmark")]` in `transport.rs`. The `inventory::submit!` and `NEXMARK_DESCRIPTOR` static therefore only compile and register when the feature is enabled — exactly the right behaviour: `connector_by_name("nexmark")` returns `None` when the feature is off, and the fallback wildcard arm returns `Ok(None)`.

### 4. No re-exports to clean up — module is self-contained

The `mod nexmark;` declaration in `transport.rs` had no accompanying `pub use nexmark::NexmarkEndpoint` re-export (unlike nats/pubsub). The only removal needed was `use crate::transport::nexmark::NexmarkEndpoint` in `transport.rs`, which was used only by the fallback match arm that is now gone.

### 5. The `#[allow(unused_variables)]` attribute on `input_transport_config_to_endpoint` becomes dead after the fallback collapse

With `_ => Ok(None)` as the only fallback arm, all named function parameters (`endpoint_name`, `secrets_dir`) are always used — either in the registry path or they're never reached in the fallback. The `#[allow(unused_variables)]` that was needed when feature-gated connectors left parameters unused in certain compilation configurations is no longer strictly necessary, though it is harmless. PR 7g should remove it.
