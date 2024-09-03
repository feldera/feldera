use crate::{
    controller::{EndpointId, InputEndpointStatus, OutputEndpointStatus},
    Controller,
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
    handle: PrometheusHandle,
}

impl PrometheusMetrics {
    pub(crate) fn new(controller: &Controller) -> AnyResult<Self> {
        let pipeline_name = controller
            .status()
            .pipeline_config
            .name
            .as_ref()
            .map_or_else(|| "unnamed".to_string(), |n| n.clone());
        let mut result = Self {
            pipeline_name,
            input_metrics: BTreeMap::new(),
            output_metrics: BTreeMap::new(),
            handle: controller.metrics(),
        };

        let status = controller.status();

        for (endpoint_id, endpoint_status) in status.input_status().iter() {
            result.add_input_endpoint(*endpoint_id, endpoint_status)?;
        }

        for (endpoint_id, endpoint_status) in status.output_status().iter() {
            result.add_output_endpoint(*endpoint_id, endpoint_status)?;
        }

        Ok(result)
    }

    pub(crate) fn add_input_endpoint(
        &mut self,
        endpoint_id: EndpointId,
        status: &InputEndpointStatus,
    ) -> AnyResult<()> {
        let total_bytes = self.create_gauge("input_total_bytes", &status.endpoint_name)?;
        let total_records = self.create_gauge("input_total_records", &status.endpoint_name)?;
        let buffered_records =
            self.create_gauge("input_buffered_records", &status.endpoint_name)?;
        let num_transport_errors =
            self.create_gauge("input_num_transport_errors", &status.endpoint_name)?;
        let num_parse_errors =
            self.create_gauge("input_num_parse_errors", &status.endpoint_name)?;

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
        status: &OutputEndpointStatus,
    ) -> AnyResult<()> {
        let transmitted_bytes =
            self.create_gauge("output_transmitted_bytes", &status.endpoint_name)?;
        let transmitted_records =
            self.create_gauge("output_transmitted_records", &status.endpoint_name)?;
        let buffered_records =
            self.create_gauge("output_buffered_records", &status.endpoint_name)?;
        let buffered_batches =
            self.create_gauge("output_buffered_batches", &status.endpoint_name)?;
        let num_transport_errors =
            self.create_gauge("output_num_transport_errors", &status.endpoint_name)?;
        let num_encode_errors =
            self.create_gauge("output_num_encode_errors", &status.endpoint_name)?;

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

        for (endpoint_id, endpoint_status) in status.input_status().iter() {
            self.update_input_metrics(*endpoint_id, endpoint_status)?;
        }

        for (endpoint_id, endpoint_status) in status.output_status().iter() {
            self.update_output_metrics(*endpoint_id, endpoint_status)?;
        }

        Ok(self.handle.render().into())
    }

    fn create_gauge(&self, name: &str, endpoint: &str) -> AnyResult<Gauge> {
        metrics::describe_gauge!(name.to_owned(), name.to_owned());
        let gauge = metrics::gauge!(
            name.to_owned(),
            "pipeline" => self.pipeline_name.clone(),
            "endpoint" => endpoint.to_owned()
        );

        Ok(gauge)
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
