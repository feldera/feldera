use colored::ColoredString;
use feldera_observability::json_logging::init_pipeline_logging;
use tracing::warn;
use tracing_subscriber::EnvFilter;

/// Initializes the logger by setting its filter and template.
/// By default, the logging level is set to `INFO`.
/// This can be overridden by setting the `RUST_LOG` environment variable.
/// Set `FELDERA_LOG_JSON=1` to emit structured JSON logs instead of pretty text.
pub fn init_logging(name: ColoredString) {
    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .expect("valid default filter");

    init_pipeline_logging(name, env_filter).unwrap_or_else(|e| {
        warn!("Unable to initialize logging -- has it already been initialized? ({e})")
    });
}
