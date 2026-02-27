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

/// Connector-specific metrics exported via the Prometheus `/metrics` endpoint.
///
/// Connectors that have metrics beyond the standard [`InputEndpointMetrics`]
/// should implement this trait and register themselves via
/// [`InputConsumer::set_custom_metrics`] during [`TransportInputEndpoint::open`].
pub trait ConnectorMetrics: Send + Sync {
    /// Return a list of `(name, help, value_type, value)` tuples.
    ///
    /// The returned `name`s must be valid Prometheus metric name components
    /// (no spaces, no special characters other than `_`).
    fn metrics(&self) -> Vec<(&'static str, &'static str, ValueType, f64)>;
}
