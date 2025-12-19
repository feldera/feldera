use chrono::Utc;
use colored::{ColoredString, Colorize};
use serde_json::{Map, Value};
use tracing::Subscriber;
use tracing_subscriber::fmt::format::{self, Format, Writer};
use tracing_subscriber::fmt::{FormatEvent, FormatFields};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

#[derive(Clone, Copy, Debug)]
pub enum ServiceName {
    Manager,
    Runner,
    CompilerServer,
    ControlPlane,
    KubernetesRunner,
    Pipeline,
    ApiServer,
}

impl ServiceName {
    pub fn as_str(self) -> &'static str {
        match self {
            ServiceName::Manager => "manager",
            ServiceName::Runner => "runner",
            ServiceName::CompilerServer => "compiler-server",
            ServiceName::ControlPlane => "control-plane",
            ServiceName::KubernetesRunner => "kubernetes-runner",
            ServiceName::Pipeline => "pipeline",
            ServiceName::ApiServer => "api-server",
        }
    }
}

impl From<ServiceName> for String {
    fn from(value: ServiceName) -> Self {
        value.as_str().to_string()
    }
}

#[derive(Clone)]
pub enum LogIdentity {
    /// Control-plane component (manager | runner | compiler-server | control-plane).
    Service { service_name: Option<ServiceName> },
    /// Pipeline log source.
    Pipeline {
        pipeline_name: Option<String>,
        pipeline_id: Option<String>,
    },
}

/// Shared JSON formatter that injects pipeline/service name and structured fields.
pub struct JsonPipelineFormat {
    identity: LogIdentity,
}

impl JsonPipelineFormat {
    pub fn new(identity: LogIdentity) -> Self {
        Self { identity }
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

        // Defaults from the configured identity.
        let mut pipeline_name = match &self.identity {
            LogIdentity::Service { .. } => None,
            LogIdentity::Pipeline {
                pipeline_name,
                pipeline_id: _,
            } => pipeline_name.clone(),
        };
        let mut pipeline_id = match &self.identity {
            LogIdentity::Service { .. } => None,
            LogIdentity::Pipeline { pipeline_id, .. } => pipeline_id.clone(),
        };
        let mut feldera_service = match &self.identity {
            LogIdentity::Service { service_name } => {
                service_name.map(|service| service.as_str().to_string())
            }
            LogIdentity::Pipeline { .. } => Some(ServiceName::Pipeline.as_str().to_string()),
        };

        // Allow structured fields to override the defaults.
        if let Some(value) = visitor.fields.remove("pipeline-name") {
            pipeline_name = value_to_string(value);
        } else if let Some(value) = visitor.fields.remove("pipeline") {
            pipeline_name = value_to_string(value);
        }
        if let Some(value) = visitor.fields.remove("pipeline-id") {
            pipeline_id = value_to_string(value);
        } else if let Some(value) = visitor.fields.remove("pipeline_id") {
            pipeline_id = value_to_string(value);
        }
        if let Some(value) = visitor.fields.remove("feldera-service") {
            feldera_service = value_to_string(value);
        }

        let metadata = event.metadata();
        // Default service tagging based on module path when logs come from shared binaries.
        if metadata.target().starts_with("pipeline_manager::runner")
            && feldera_service.as_deref() != Some("runner")
        {
            feldera_service = Some("runner".to_string());
        } else if metadata.target().starts_with("pipeline_manager::compiler")
            && feldera_service.as_deref() != Some("compiler-server")
        {
            feldera_service = Some("compiler-server".to_string());
        } else if metadata
            .target()
            .starts_with("cluster_control_plane::kubernetes_runner")
            && feldera_service.as_deref() != Some("kubernetes-runner")
        {
            feldera_service = Some("kubernetes-runner".to_string());
        }
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
        if let Some(service_name) = feldera_service {
            obj.insert("feldera-service".to_string(), Value::String(service_name));
        }
        if let Some(pipeline_name) = pipeline_name {
            obj.insert("pipeline-name".to_string(), Value::String(pipeline_name));
        }
        if let Some(pipeline_id) = pipeline_id {
            obj.insert("pipeline-id".to_string(), Value::String(pipeline_id));
        }
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
    init_pipeline_logging_with_id(pipeline_name, None, env_filter)
}

/// Initialize logging with either the JSON or text pipeline format, optionally attaching a pipeline id.
pub fn init_pipeline_logging_with_id(
    pipeline_name: ColoredString,
    pipeline_id: Option<String>,
    env_filter: EnvFilter,
) -> Result<(), tracing_subscriber::util::TryInitError> {
    init_logging(
        pipeline_name,
        LogIdentity::Pipeline {
            pipeline_name: None,
            pipeline_id,
        },
        env_filter,
    )
}

/// Initialize logging for a control-plane service (manager | runner | compiler-server | control-plane).
pub fn init_service_logging(
    service_name: ColoredString,
    feldera_service: ServiceName,
    env_filter: EnvFilter,
) -> Result<(), tracing_subscriber::util::TryInitError> {
    init_logging(
        service_name,
        LogIdentity::Service {
            service_name: Some(feldera_service),
        },
        env_filter,
    )
}

fn init_logging(
    prefix: ColoredString,
    identity: LogIdentity,
    env_filter: EnvFilter,
) -> Result<(), tracing_subscriber::util::TryInitError> {
    let identity = match identity {
        LogIdentity::Service { service_name } => LogIdentity::Service {
            service_name: service_name.or(Some(ServiceName::ControlPlane)),
        },
        LogIdentity::Pipeline {
            pipeline_name,
            pipeline_id,
        } => LogIdentity::Pipeline {
            pipeline_name: Some(
                pipeline_name
                    .unwrap_or_else(|| sanitize_pipeline_name(prefix.clone().clear().to_string())),
            ),
            pipeline_id,
        },
    };

    if use_json_log_format() {
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .event_format(JsonPipelineFormat::new(identity))
                    .with_ansi(false),
            )
            .with(env_filter)
            .with(sentry::integrations::tracing::layer())
            .try_init()
    } else {
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .event_format(FormatWithPrefix::new(prefix))
                    .fmt_fields(plain_text_fields()),
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

fn value_to_string(value: Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Null => None,
        other => Some(other.to_string()),
    }
}

/// In plain text, render the message plus selected structured fields.
fn plain_text_fields() -> impl for<'writer> tracing_subscriber::fmt::FormatFields<'writer> + Clone {
    format::debug_fn(|writer, field, value| {
        match field.name() {
            "message" => {
                write!(writer, " {value:?}")?;
            }
            "pipeline" | "pipeline_name" | "pipeline-name" => {
                write!(writer, " pipeline-name={value:?}")?;
            }
            "pipeline_id" | "pipeline-id" => {
                write!(writer, " pipeline-id={value:?}")?;
            }
            "tenant" => {
                write!(writer, " tenant={value:?}")?;
            }
            _ => {}
        }
        Ok(())
    })
}
