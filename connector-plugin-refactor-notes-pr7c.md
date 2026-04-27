# PR 7c Notes — redis (output-only)

## Unexpected findings

### 1. First output-only descriptor with `Direction::Output`
Previous connectors (file_output aside) were input-only. Redis is the first pure-output regular connector migrated. The `build_output` signature takes `fault_tolerant: bool` but redis ignores it — `RedisOutputEndpoint::is_fault_tolerant()` unconditionally returns `false`. No branching needed in the factory.

### 2. redis/output.rs already imports `OutputEndpoint` and `AnyResult`
Unlike pubsub/input.rs (which needed a new `use serde_json::Value as JsonValue`), the redis output file already imports `OutputEndpoint` from `feldera_adapterlib::transport` and `AnyResult` from `anyhow`. Only `ConnectorDescriptor`/`ConnectorFlags`/`ConnectorKind`/`Direction` and `serde_json::Value` were new.

### 3. The redis wrapper module (redis.rs) does NOT re-export the endpoint
Unlike nats.rs and pubsub.rs (which had dead `pub use input::NatsInputEndpoint` / `pub use input::PubSubInputEndpoint` to remove), redis.rs only declares `pub mod output;` — no re-export ever existed. Nothing to clean up in the wrapper.

### 4. Feature-gated match arm in output_transport_config_to_endpoint eliminated
The fallback for redis was `#[cfg(feature = "with-redis")] TransportConfig::RedisOutput(config) => Ok(Some(Box::new(...)))`. After migration both the feature-gated arm and its matching `use redis::output::RedisOutputEndpoint` import are removed. The catch-all `_ => Ok(None)` handles the feature-disabled case automatically, same pattern as nats/pubsub input connectors.
