use feldera_storage::histogram::ExponentialHistogramSnapshot;

/// The type of a connector metric.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ValueType {
    /// A monotonically increasing counter.
    Counter,
    /// An instantaneous gauge.
    Gauge,
}

impl ValueType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ValueType::Counter => "counter",
            ValueType::Gauge => "gauge",
        }
    }
}

/// A connector histogram metric.
///
/// The histogram is exported in the unit it was recorded in, so `name` should
/// name that unit (for example, a histogram of microseconds should be named
/// `..._microseconds`).
pub struct ConnectorHistogram {
    /// Metric name; must be a valid Prometheus metric name component (no spaces,
    /// no special characters other than `_`).
    pub name: &'static str,

    /// Human-readable help text.
    pub help: &'static str,

    /// The recorded histogram.
    pub snapshot: ExponentialHistogramSnapshot,
}

/// Connector-specific metrics exported via the Prometheus `/metrics` endpoint.
///
/// Connectors that have metrics beyond the standard `InputEndpointMetrics`
/// should implement this trait and register themselves via
/// `InputConsumer::set_custom_metrics` during `TransportInputEndpoint::open`.
pub trait ConnectorMetrics: Send + Sync {
    /// Return a list of `(name, help, value_type, value)` tuples.
    ///
    /// The returned `name`s must be valid Prometheus metric name components
    /// (no spaces, no special characters other than `_`).
    fn metrics(&self) -> Vec<(&'static str, &'static str, ValueType, f64)>;

    /// Return the connector's histogram metrics.
    ///
    /// Defaults to an empty list, so connectors that export only scalar metrics
    /// need not implement it.
    fn histograms(&self) -> Vec<ConnectorHistogram> {
        Vec::new()
    }
}
