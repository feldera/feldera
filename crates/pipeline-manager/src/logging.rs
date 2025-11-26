use colored::{ColoredString, Colorize};
use feldera_observability::json_logging::{
    sanitize_pipeline_name, use_json_log_format, JsonPipelineFormat,
};
use tracing::warn;
use tracing::Subscriber;
use tracing_subscriber::fmt::format::Format;
use tracing_subscriber::fmt::{FormatEvent, FormatFields};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

/// Initializes the logger by setting its filter and template.
/// By default, the logging level is set to `INFO`.
/// This can be overridden by setting the `RUST_LOG` environment variable.
/// Set `RUST_LOG_JSON=1` to emit structured JSON logs instead of pretty text.
pub fn init_logging(name: ColoredString) {
    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .expect("valid default filter");

    if use_json_log_format() {
        let pipeline_name_json = sanitize_pipeline_name(name.clone().clear().to_string());
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .event_format(JsonPipelineFormat::new(pipeline_name_json))
                    .with_ansi(false),
            )
            .with(env_filter)
            .with(sentry::integrations::tracing::layer())
            .try_init()
    } else {
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().event_format(ManagerFormat::new(name)))
            .with(env_filter)
            .with(sentry::integrations::tracing::layer())
            .try_init()
    }
    .unwrap_or_else(|e| {
        warn!("Unable to initialize logging -- has it already been initialized? ({e})")
    });
}

struct ManagerFormat {
    name: ColoredString,
    inner: Format,
}

impl ManagerFormat {
    fn new(name: ColoredString) -> Self {
        Self {
            name,
            inner: Format::default(),
        }
    }
}

impl<S, N> FormatEvent<S, N> for ManagerFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &tracing_subscriber::fmt::FmtContext<'_, S, N>,
        mut writer: tracing_subscriber::fmt::format::Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        write!(writer, "{} ", self.name)?;
        self.inner.format_event(ctx, writer, event)
    }
}
