# Connector Plugin Refactor — PR 7 Notes

## PR 7b (nats + pubsub)

### 1. Feature-gated `#[cfg(not(feature = "..."))]` fallback arms are eliminated, not migrated

The plan's migration recipe says to move connector arms into the `Ok(None)` catch arm.
For nats and pubsub the fallback match had *two* arms each: a feature-enabled arm that
constructed the endpoint, and a `#[cfg(not(feature = "..."))]` arm that returned
`Ok(None)` for the case when the feature is disabled.

After migration, **both** arms are removed and both code paths are subsumed:
- Feature enabled: registry dispatch fires, returns early before the fallback match.
- Feature disabled: no descriptor registered, falls through to the catch arm,
  returns `Ok(None)` unconditionally — no `#[cfg]` needed.

The `Ok(None)` catch arm is feature-unconditional, so it silently handles the disabled
case for free. The `#[cfg(not(feature = "..."))]` arms were there purely to keep the
fallback match exhaustive when the impl types didn't exist; they become unnecessary
once the arms are gone.

**Plan update**: the migration recipe should note this for any feature-gated connector
in Phase 4b: remove both the `#[cfg(feature)]` and `#[cfg(not(feature))]` arms; the
catch arm handles the disabled case automatically.

### 2. Re-exports in wrapper modules become dead after migration

`nats.rs` and `pubsub.rs` are thin wrapper modules that only declare `mod input;` and
`pub use input::NatsInputEndpoint` / `pub use input::PubSubInputEndpoint`. Those
re-exports existed solely to make the endpoint types visible to `transport.rs` for the
fallback match. After migration, transport.rs no longer names these concrete types at
all — the build functions in the connector modules construct them directly.

Both re-exports produced unused-import warnings and were removed, leaving the wrapper
modules as pure namespace containers for `mod input` (and `mod test` for pubsub).

**Pattern for 7c–7f**: after migrating a connector, check the module's wrapper file
for re-exports that only existed to feed the fallback match. They can be deleted.

### 3. `pub_sub_input` — name with embedded underscores between each word

The `name()` return value for `PubSubInput` is `"pub_sub_input"` (three words, two
underscores), not `"pubsub_input"`. This is set in `feldera-types/src/config.rs` and
is what the descriptor `name` field must match exactly. The serde tag could differ; the
registry lookup uses `TransportConfig::name()`, not the serde tag. No mismatch here,
but worth flagging for anyone adding a descriptor without reading the `name()` impl.

### 4. `pub_sub_input` registry test runs in the default build without live GCP

The pubsub implementation tests (`pubsub/test.rs`) are gated behind
`pubsub-emulator-test` / `pubsub-gcp-test` features and require live infrastructure.
The new `registry_test::pub_sub_input_descriptor` test (added in `pubsub/input.rs`)
uses only `connector_by_name` and runs cleanly in the default build with no external
dependencies. This is the correct place for it — the infrastructure tests stay gated,
the registry test doesn't need to be.
