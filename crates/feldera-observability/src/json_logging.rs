use chrono::Utc;
use colored::{ColoredString, Colorize};
use serde_json::{Map, Value};
use tracing::Subscriber;
use tracing_subscriber::fmt::format::{Format, Writer};
use tracing_subscriber::fmt::{FormatEvent, FormatFields};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

/// Shared JSON formatter that injects pipeline/service name and structured fields.
pub struct JsonPipelineFormat {
    pipeline: String,
}

impl JsonPipelineFormat {
    pub fn new(pipeline: String) -> Self {
        Self { pipeline }
    }
}

impl<S, N> FormatEvent<S, N> for JsonPipelineFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &tracing_subscriber::fmt::FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        let mut visitor = SerdeVisitor::default();
        event.record(&mut visitor);

        let metadata = event.metadata();
        let mut obj = Map::new();
        obj.insert("timestamp".to_string(), Value::String(now_timestamp()));
        obj.insert(
            "level".to_string(),
            Value::String(metadata.level().as_str().to_string()),
        );
        obj.insert(
            "target".to_string(),
            Value::String(metadata.target().to_string()),
        );
        obj.insert("pipeline".to_string(), Value::String(self.pipeline.clone()));
        obj.insert("fields".to_string(), Value::Object(visitor.fields));

        if let Some(span) = ctx.lookup_current() {
            obj.insert("span".to_string(), Value::String(span.name().to_string()));
        }

        let json = Value::Object(obj);
        writeln!(writer, "{}", json)
    }
}

#[derive(Default)]
pub struct SerdeVisitor {
    pub fields: Map<String, Value>,
}

impl tracing::field::Visit for SerdeVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.fields.insert(
            field.name().to_string(),
            Value::String(format!("{value:?}")),
        );
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        let as_number = serde_json::Number::from_f64(value);
        let val = as_number
            .map(Value::Number)
            .unwrap_or_else(|| Value::String(value.to_string()));
        self.fields.insert(field.name().to_string(), val);
    }

    fn record_i128(&mut self, field: &tracing::field::Field, value: i128) {
        let val = i64::try_from(value)
            .map(|v| Value::Number(v.into()))
            .unwrap_or_else(|_| Value::String(value.to_string()));
        self.fields.insert(field.name().to_string(), val);
    }

    fn record_u128(&mut self, field: &tracing::field::Field, value: u128) {
        let val = u64::try_from(value)
            .map(|v| Value::Number(v.into()))
            .unwrap_or_else(|_| Value::String(value.to_string()));
        self.fields.insert(field.name().to_string(), val);
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.fields
            .insert(field.name().to_string(), Value::String(value.to_string()));
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.fields
            .insert(field.name().to_string(), Value::Number(value.into()));
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.fields
            .insert(field.name().to_string(), Value::Number(value.into()));
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.fields
            .insert(field.name().to_string(), Value::Bool(value));
    }
}

fn now_timestamp() -> String {
    Utc::now().format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()
}

/// True when structured JSON logging is requested via `FELDERA_LOG_JSON`.
pub fn use_json_log_format() -> bool {
    matches!(
        std::env::var("FELDERA_LOG_JSON")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// Remove surrounding `[]` often used in pretty log prefixes.
pub fn sanitize_pipeline_name(name: impl AsRef<str>) -> String {
    name.as_ref()
        .trim_matches(|c| c == '[' || c == ']')
        .to_string()
}

/// Initialize logging with either the JSON or text pipeline format.
pub fn init_pipeline_logging(
    pipeline_name: ColoredString,
    env_filter: EnvFilter,
) -> Result<(), tracing_subscriber::util::TryInitError> {
    if use_json_log_format() {
        let pipeline_name_json = sanitize_pipeline_name(pipeline_name.clone().clear().to_string());
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
            .with(
                tracing_subscriber::fmt::layer().event_format(FormatWithPrefix::new(pipeline_name)),
            )
            .with(env_filter)
            .with(sentry::integrations::tracing::layer())
            .try_init()
    }
}

/// Text formatter that prepends the pipeline name to each event.
pub struct FormatWithPrefix {
    pipeline_name: ColoredString,
    inner: Format,
}

impl FormatWithPrefix {
    pub fn new(pipeline_name: ColoredString) -> Self {
        Self {
            pipeline_name,
            inner: Format::default(),
        }
    }
}

impl<S, N> FormatEvent<S, N> for FormatWithPrefix
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &tracing_subscriber::fmt::FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        write!(writer, "{} ", self.pipeline_name)?;
        self.inner.format_event(ctx, writer, event)
    }
}
