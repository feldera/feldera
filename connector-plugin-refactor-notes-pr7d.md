# PR 7d Notes — kafka (input + output)

## Unexpected findings

### 1. Plan's `Direction::InputOutput` / "same descriptor" claim doesn't hold

The plan says PR 7d is "the first connector with `Direction::InputOutput` (e.g. kafka in PR 7d — same descriptor used from both factories)". In practice this is impossible: `TransportConfig::name()` returns `"kafka_input"` for `KafkaInput` and `"kafka_output"` for `KafkaOutput`. Registry lookup is by `name`, so a single descriptor with a single name cannot be found by both factory calls. Two separate descriptors are required — `KAFKA_INPUT_DESCRIPTOR` (name="kafka_input") and `KAFKA_OUTPUT_DESCRIPTOR` (name="kafka_output"). `Direction::Input` and `Direction::Output` respectively; no `Direction::InputOutput` appears anywhere.

### 2. The ft/nonft split is absorbed entirely inside `build_kafka_output`

The plan notes kafka is special because `fault_tolerant: bool` must dispatch between `KafkaFtOutputEndpoint` (FT) and `KafkaOutputEndpoint` (non-FT). This works cleanly: the single `build_kafka_output` function receives `fault_tolerant` and branches on it. The descriptor's `fault_tolerance: Some(FtModel::ExactlyOnce)` correctly represents the best-case capability, since FT output is available when requested.

### 3. The kafka re-exports in `kafka.rs` survive unchanged

`kafka.rs` had `pub use ft::{KafkaFtInputEndpoint, KafkaFtOutputEndpoint}` and `pub use nonft::KafkaOutputEndpoint` whose only consumer was the fallback match in `transport.rs`. After migration those imports in `transport.rs` are removed. However, `build_kafka_input` and `build_kafka_output` in `kafka.rs` itself now call these endpoint types directly — so the `pub use` re-exports remain in active use and generate no warnings. Unlike nats/pubsub where re-exports went dead, kafka's re-exports stay live because the registry block lives in the same file.

### 4. `_secrets_dir` is still ignored for kafka despite the plan claiming otherwise

The plan states: "Only kafka consumes both: `secrets_dir` for TLS / SASL credential file paths." However, the pre-migration fallback never passed `secrets_dir` to `KafkaFtInputEndpoint::new()` or the output constructors — secrets resolution happens entirely in `resolve_secret_references_via_json` at the factory entry, before the build function is called. The build functions receive an already-resolved config value. So `_secrets_dir` is prefixed with underscore (unused), consistent with all other connectors.

### 5. Output fallback match becomes trivially `_ => Ok(None)` after kafka migrates

After redis (PR 7c) and kafka (PR 7d) both migrate, the output fallback match has no remaining explicit arms — only `_ => Ok(None)`. This is the first factory function to reach that state before PR 7g.
