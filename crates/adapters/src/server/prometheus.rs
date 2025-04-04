use crate::{
    controller::{EndpointId, InputEndpointStatus, OutputEndpointStatus},
    Controller, ControllerStatus,
};
use anyhow::{Error as AnyError, Result as AnyResult};
use metrics::Gauge;
use metrics_exporter_prometheus::PrometheusHandle;
use std::{collections::BTreeMap, sync::atomic::Ordering};

/// Prometheus metrics of the controller.
///
/// The primary metrics are stored in `controller.status` and are mirrored
/// to Prometheus metrics on demand.
pub(crate) struct PrometheusMetrics {
    pipeline_name: String,
    input_metrics: BTreeMap<EndpointId, InputMetrics>,
    output_metrics: BTreeMap<EndpointId, OutputMetrics>,
    global_metrics: GlobalMetrics,
    handle: PrometheusHandle,
}

impl PrometheusMetrics {
    pub(crate) fn new(controller: &Controller) -> AnyResult<Self> {
        let status = controller.status();

        let pipeline_name = status
            .pipeline_config
            .name
            .as_ref()
            .map_or_else(|| "unnamed".to_string(), |n| n.clone());

        let global_metrics = GlobalMetrics {
            cpu_msecs: GaugeBuilder::new("cpu_msecs", pipeline_name.clone())
                .with_description("cpu time used by the pipeline process in milliseconds")
                .with_unit(metrics::Unit::Milliseconds)
                .build(),
            rss_bytes: GaugeBuilder::new("rss_bytes", pipeline_name.clone())
                .with_description("resident set size of the pipeline process in bytes")
                .with_unit(metrics::Unit::Bytes)
                .build(),
            buffered_input_records: GaugeBuilder::new(
                "buffered_input_records",
                pipeline_name.clone(),
            )
            .with_description("total number of records currently buffered by all endpoints")
            .with_unit(metrics::Unit::Count)
            .build(),
            total_input_records: GaugeBuilder::new("total_input_records", pipeline_name.clone())
                .with_description("total number of input records received from all connectors")
                .with_unit(metrics::Unit::Count)
                .build(),
            total_processed_records: GaugeBuilder::new(
                "total_processed_records",
                pipeline_name.clone(),
            )
            .with_description("total number of input records processed by the pipeline")
            .with_unit(metrics::Unit::Count)
            .build(),
            pipeline_completed: GaugeBuilder::new("pipeline_completed", pipeline_name.clone())
                .with_description("boolean, 1 if true")
                .build(),
        };

        let mut result = Self {
            pipeline_name,
            input_metrics: BTreeMap::new(),
            output_metrics: BTreeMap::new(),
            handle: controller.metrics(),
            global_metrics,
        };

        let status = controller.status();

        for (endpoint_id, endpoint_status) in status.input_status().iter() {
            result.add_input_endpoint(*endpoint_id, &endpoint_status.endpoint_name)?;
        }

        for (endpoint_id, endpoint_status) in status.output_status().iter() {
            result.add_output_endpoint(*endpoint_id, &endpoint_status.endpoint_name)?;
        }

        Ok(result)
    }

    pub(crate) fn add_input_endpoint(
        &mut self,
        endpoint_id: EndpointId,
        endpoint_name: &str,
    ) -> AnyResult<()> {
        let total_bytes = GaugeBuilder::new("input_total_bytes", self.pipeline_name.clone())
            .with_unit(metrics::Unit::Bytes)
            .with_endpoint(String::from(endpoint_name))
            .build();
        let total_records = GaugeBuilder::new("input_total_records", self.pipeline_name.clone())
            .with_endpoint(String::from(endpoint_name))
            .with_unit(metrics::Unit::Count)
            .build();
        let buffered_records =
            GaugeBuilder::new("input_buffered_records", self.pipeline_name.clone())
                .with_endpoint(String::from(endpoint_name))
                .with_unit(metrics::Unit::Count)
                .build();
        let num_transport_errors =
            GaugeBuilder::new("input_num_transport_errors", self.pipeline_name.clone())
                .with_endpoint(String::from(endpoint_name))
                .with_unit(metrics::Unit::Count)
                .build();
        let num_parse_errors =
            GaugeBuilder::new("input_num_parse_errors", self.pipeline_name.clone())
                .with_endpoint(String::from(endpoint_name))
                .with_unit(metrics::Unit::Count)
                .build();

        let input_metrics = InputMetrics {
            total_bytes,
            total_records,
            buffered_records,
            num_transport_errors,
            num_parse_errors,
        };

        self.input_metrics.insert(endpoint_id, input_metrics);
        Ok(())
    }

    pub(crate) fn update_input_metrics(
        &self,
        endpoint_id: EndpointId,
        status: &InputEndpointStatus,
    ) -> AnyResult<()> {
        let metrics = self.input_metrics.get(&endpoint_id).ok_or_else(|| {
            AnyError::msg(format!("Missing metrics for input endpoint {endpoint_id}"))
        })?;

        metrics
            .total_bytes
            .set(status.metrics.total_bytes.load(Ordering::Acquire) as f64);
        metrics
            .total_records
            .set(status.metrics.total_records.load(Ordering::Acquire) as f64);
        metrics
            .buffered_records
            .set(status.metrics.buffered_records.load(Ordering::Acquire) as f64);
        metrics
            .num_transport_errors
            .set(status.metrics.num_transport_errors.load(Ordering::Acquire) as f64);
        metrics
            .num_parse_errors
            .set(status.metrics.num_parse_errors.load(Ordering::Acquire) as f64);

        Ok(())
    }

    pub(crate) fn add_output_endpoint(
        &mut self,
        endpoint_id: EndpointId,
        endpoint_name: &str,
    ) -> AnyResult<()> {
        let transmitted_bytes =
            GaugeBuilder::new("output_transmitted_bytes", self.pipeline_name.clone())
                .with_endpoint(String::from(endpoint_name))
                .with_unit(metrics::Unit::Bytes)
                .build();
        let transmitted_records =
            GaugeBuilder::new("output_transmitted_records", self.pipeline_name.clone())
                .with_endpoint(String::from(endpoint_name))
                .with_unit(metrics::Unit::Count)
                .build();
        let buffered_records =
            GaugeBuilder::new("output_buffered_records", self.pipeline_name.clone())
                .with_endpoint(String::from(endpoint_name))
                .with_unit(metrics::Unit::Count)
                .build();
        let buffered_batches =
            GaugeBuilder::new("output_buffered_batches", self.pipeline_name.clone())
                .with_endpoint(String::from(endpoint_name))
                .with_unit(metrics::Unit::Count)
                .build();
        let num_transport_errors =
            GaugeBuilder::new("output_num_transport_errors", self.pipeline_name.clone())
                .with_endpoint(String::from(endpoint_name))
                .with_unit(metrics::Unit::Count)
                .build();
        let num_encode_errors =
            GaugeBuilder::new("output_num_encode_errors", self.pipeline_name.clone())
                .with_endpoint(String::from(endpoint_name))
                .with_unit(metrics::Unit::Count)
                .build();

        let output_metrics = OutputMetrics {
            transmitted_bytes,
            transmitted_records,
            buffered_records,
            buffered_batches,
            num_transport_errors,
            num_encode_errors,
        };

        self.output_metrics.insert(endpoint_id, output_metrics);
        Ok(())
    }

    pub(crate) fn update_output_metrics(
        &self,
        endpoint_id: EndpointId,
        status: &OutputEndpointStatus,
    ) -> AnyResult<()> {
        let metrics = self.output_metrics.get(&endpoint_id).ok_or_else(|| {
            AnyError::msg(format!("Missing metrics for output endpoint {endpoint_id}"))
        })?;

        metrics
            .transmitted_bytes
            .set(status.metrics.transmitted_bytes.load(Ordering::Acquire) as f64);
        metrics
            .transmitted_records
            .set(status.metrics.transmitted_records.load(Ordering::Acquire) as f64);
        metrics
            .buffered_records
            .set(status.metrics.queued_records.load(Ordering::Acquire) as f64);
        metrics
            .buffered_batches
            .set(status.metrics.queued_batches.load(Ordering::Acquire) as f64);
        metrics
            .num_transport_errors
            .set(status.metrics.num_transport_errors.load(Ordering::Acquire) as f64);
        metrics
            .num_encode_errors
            .set(status.metrics.num_encode_errors.load(Ordering::Acquire) as f64);

        Ok(())
    }

    /// Extract metrics in the format expected by the Prometheus server.
    pub(crate) fn metrics(&self, controller: &Controller) -> AnyResult<Vec<u8>> {
        let status = controller.status();

        self.global_metrics.update(status);

        for (endpoint_id, endpoint_status) in status.input_status().iter() {
            self.update_input_metrics(*endpoint_id, endpoint_status)?;
        }

        for (endpoint_id, endpoint_status) in status.output_status().iter() {
            self.update_output_metrics(*endpoint_id, endpoint_status)?;
        }

        Ok(self.handle.render().into())
    }
}

struct GaugeBuilder {
    pipeline_name: String,
    name: &'static str,
    description: Option<&'static str>,
    unit: Option<metrics::Unit>,
    endpoint: Option<String>,
}

impl GaugeBuilder {
    fn new(name: &'static str, pipeline_name: String) -> GaugeBuilder {
        Self {
            pipeline_name,
            name,
            description: None,
            unit: None,
            endpoint: None,
        }
    }

    fn with_description(mut self, description: &'static str) -> GaugeBuilder {
        self.description = Some(description);
        self
    }

    fn with_unit(mut self, unit: metrics::Unit) -> GaugeBuilder {
        self.unit = Some(unit);
        self
    }

    fn with_endpoint(mut self, endpoint: String) -> GaugeBuilder {
        self.endpoint = Some(endpoint);
        self
    }

    fn build(mut self) -> Gauge {
        let pipeline_name = std::mem::take(&mut self.pipeline_name);
        let description = self.description.unwrap_or_default().to_owned();
        let mut labels = vec![("pipeline".to_owned(), pipeline_name)];

        if let Some(unit) = self.unit {
            metrics::describe_gauge!(self.name, unit, description);
        } else {
            metrics::describe_gauge!(self.name, description);
        }

        if let Some(endpoint) = self.endpoint {
            labels.push(("endpoint".to_owned(), endpoint.to_owned()));
        }

        metrics::gauge!(self.name, &labels)
    }
}

struct InputMetrics {
    total_bytes: Gauge,
    total_records: Gauge,
    buffered_records: Gauge,
    num_transport_errors: Gauge,
    num_parse_errors: Gauge,
}

struct OutputMetrics {
    transmitted_bytes: Gauge,
    transmitted_records: Gauge,
    buffered_records: Gauge,
    buffered_batches: Gauge,
    num_transport_errors: Gauge,
    num_encode_errors: Gauge,
}

struct GlobalMetrics {
    cpu_msecs: Gauge,
    rss_bytes: Gauge,
    buffered_input_records: Gauge,
    total_input_records: Gauge,
    total_processed_records: Gauge,
    pipeline_completed: Gauge,
}

impl GlobalMetrics {
    fn update(&self, status: &ControllerStatus) {
        self.cpu_msecs.set(status.global_metrics.cpu_msecs() as f64);

        self.rss_bytes.set(status.global_metrics.rss_bytes() as f64);

        self.pipeline_completed
            .set(f64::from(status.pipeline_complete()));
        self.total_processed_records
            .set(status.num_total_processed_records() as f64);
        self.total_input_records
            .set(status.num_total_input_records() as f64);
        self.buffered_input_records
            .set(status.num_buffered_input_records() as f64);
    }
}
