use crate::{
    controller::{EndpointId, InputEndpointStatus, OutputEndpointStatus},
    Controller,
};
use anyhow::{Error as AnyError, Result as AnyResult};
use prometheus::{Encoder, IntGauge, Opts, Registry, TextEncoder};
use std::{collections::BTreeMap, sync::atomic::Ordering};

/// Prometheus metrics of the controller.
///
/// The primary metrics are stored in `controller.status` and are mirrored
/// to Prometheus metrics on demand.
pub(crate) struct PrometheusMetrics {
    registry: Registry,
    input_metrics: BTreeMap<EndpointId, InputMetrics>,
    output_metrics: BTreeMap<EndpointId, OutputMetrics>,
}

impl PrometheusMetrics {
    pub(crate) fn new(controller: &Controller) -> AnyResult<Self> {
        let mut result = Self {
            registry: Registry::new(),
            input_metrics: BTreeMap::new(),
            output_metrics: BTreeMap::new(),
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
        let buffered_bytes = self.create_gauge("input_buffered_bytes", &status.endpoint_name)?;
        let buffered_records =
            self.create_gauge("input_buffered_records", &status.endpoint_name)?;
        let num_transport_errors =
            self.create_gauge("input_num_transport_errors", &status.endpoint_name)?;
        let num_parse_errors =
            self.create_gauge("input_num_parse_errors", &status.endpoint_name)?;

        let input_metrics = InputMetrics {
            total_bytes,
            total_records,
            buffered_bytes,
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
            .set(status.metrics.total_bytes.load(Ordering::Acquire) as i64);
        metrics
            .total_records
            .set(status.metrics.total_records.load(Ordering::Acquire) as i64);
        metrics
            .buffered_bytes
            .set(status.metrics.buffered_bytes.load(Ordering::Acquire) as i64);
        metrics
            .buffered_records
            .set(status.metrics.buffered_records.load(Ordering::Acquire) as i64);
        metrics
            .num_transport_errors
            .set(status.metrics.num_transport_errors.load(Ordering::Acquire) as i64);
        metrics
            .num_parse_errors
            .set(status.metrics.num_parse_errors.load(Ordering::Acquire) as i64);

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
            .set(status.metrics.transmitted_bytes.load(Ordering::Acquire) as i64);
        metrics
            .transmitted_records
            .set(status.metrics.transmitted_records.load(Ordering::Acquire) as i64);
        metrics
            .buffered_records
            .set(status.metrics.queued_records.load(Ordering::Acquire) as i64);
        metrics
            .buffered_batches
            .set(status.metrics.queued_batches.load(Ordering::Acquire) as i64);
        metrics
            .num_transport_errors
            .set(status.metrics.num_transport_errors.load(Ordering::Acquire) as i64);
        metrics
            .num_encode_errors
            .set(status.metrics.num_encode_errors.load(Ordering::Acquire) as i64);

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

        let mut buffer = vec![];
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        encoder.encode(&metric_families, &mut buffer)?;

        Ok(buffer)
    }

    fn create_gauge(&self, name: &str, endpoint: &str) -> AnyResult<IntGauge> {
        let opts = Opts::new(name, name).const_label("endpoint", endpoint);
        let gauge = IntGauge::with_opts(opts)?;
        self.registry.register(Box::new(gauge.clone()))?;

        Ok(gauge)
    }
}

struct InputMetrics {
    total_bytes: IntGauge,
    total_records: IntGauge,
    buffered_bytes: IntGauge,
    buffered_records: IntGauge,
    num_transport_errors: IntGauge,
    num_parse_errors: IntGauge,
}

struct OutputMetrics {
    transmitted_bytes: IntGauge,
    transmitted_records: IntGauge,
    buffered_records: IntGauge,
    buffered_batches: IntGauge,
    num_transport_errors: IntGauge,
    num_encode_errors: IntGauge,
}
