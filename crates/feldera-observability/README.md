# feldera-observability

Shared observability helpers used across Feldera services (logging formats, middleware, utilities).

## JSON logging

The `json_logging` module provides a structured formatter and helpers so services can emit JSON logs when requested:
- `JsonPipelineFormat`: tracing formatter that injects `timestamp`, `level`, `target`, `pipeline`, `fields`, and optional `span`.
- `use_json_log_format()`: checks `RUST_LOG_JSON` (`1/true/yes/on`) to decide between text and JSON.
- `sanitize_pipeline_name()`: strips surrounding `[]` often used in colored/text prefixes.

Example usage inside a tracing setup:

```rust
use feldera_observability::json_logging::{
    JsonPipelineFormat, sanitize_pipeline_name, use_json_log_format,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

if use_json_log_format() {
    let pipeline = sanitize_pipeline_name("[my-pipeline]");
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().event_format(JsonPipelineFormat::new(pipeline)))
        .with(env_filter)
        .try_init()
        .expect("init tracing");
} else {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer()) // your text formatter here
        .with(env_filter)
        .try_init()
        .expect("init tracing");
}
```

To enable JSON logs at runtime, set `RUST_LOG_JSON=1` (or `true/yes/on`).
Timestamps are RFC3339 UTC with microsecond precision to match the text formatter.
Numerics, booleans, strings, and spans are preserved as structured JSON fields.
