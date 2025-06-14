use feldera_rest_api::types::CompilationProfile;
use feldera_rest_api::Client;
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::cmp;
use tabled::builder::Builder;
use tabled::settings::Style;
use tokio::time::{sleep, Duration, Instant};

use crate::cli::{OutputFormat, PipelineAction};

pub fn human_readable_bytes(n: i64) -> String {
    const DELIMITER: f64 = 1024_f64;
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];

    let n: f64 = n as f64;
    assert!(n >= 0f64, "No negative bytes");

    let n = n.abs();
    if n < 1_f64 {
        return format!("{} {}", n, "B");
    }
    let exp = cmp::min(
        (n.ln() / DELIMITER.ln()).floor() as i32,
        (UNITS.len() - 1) as i32,
    );
    let bytes = format!("{:.2}", n / DELIMITER.powi(exp))
        .parse::<f64>()
        .unwrap()
        * 1_f64;
    let unit = UNITS[exp as usize];
    format!("{} {}", bytes, unit)
}

/// Raw metrics collected from a single measurement
#[derive(Debug)]
struct RawMetrics {
    rss_bytes: i64,
    uptime_msecs: i64,
    incarnation_uuid: String,
    storage_bytes: i64,
    total_processed_records: i64,
    input_bytes: i64,
    input_errors: bool,
}

/// JSON representation of input connector metrics
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct InputMetrics {
    buffered_records: i64,
    end_of_input: bool,
    num_parse_errors: i64,
    num_transport_errors: i64,
    total_bytes: i64,
    total_records: i64,
}

/// JSON representation of an input connector
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct InputConnector {
    barrier: bool,
    config: Map<String, Value>,
    endpoint_name: String,
    fatal_error: Option<String>,
    metrics: InputMetrics,
    paused: bool,
}

/// JSON representation of pipeline stats
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct PipelineStats {
    global_metrics: Map<String, Value>,
    inputs: Vec<InputConnector>,
    outputs: Vec<Value>,
    suspend_error: Option<Value>,
}

impl From<&Map<String, Value>> for RawMetrics {
    fn from(stats: &Map<String, Value>) -> Self {
        let pipeline_stats: PipelineStats =
            serde_json::from_value(Value::Object(stats.clone())).unwrap();
        let global_metrics = &pipeline_stats.global_metrics;

        // Check for input errors
        let input_errors = pipeline_stats.inputs.iter().any(|input| {
            input.fatal_error.is_some()
                || input.metrics.num_parse_errors > 0
                || input.metrics.num_transport_errors > 0
        });

        // Calculate total input bytes
        let input_bytes = pipeline_stats
            .inputs
            .iter()
            .map(|input| input.metrics.total_bytes)
            .sum();

        Self {
            rss_bytes: global_metrics
                .get("rss_bytes")
                .and_then(|v| v.as_i64())
                .unwrap(),
            uptime_msecs: global_metrics
                .get("uptime_msecs")
                .and_then(|v| v.as_i64())
                .unwrap(),
            incarnation_uuid: global_metrics
                .get("incarnation_uuid")
                .and_then(|v| v.as_str())
                .unwrap()
                .to_string(),
            storage_bytes: global_metrics
                .get("storage_bytes")
                .and_then(|v| v.as_i64())
                .unwrap(),
            total_processed_records: global_metrics
                .get("total_processed_records")
                .and_then(|v| v.as_i64())
                .unwrap(),
            input_bytes,
            input_errors,
        }
    }
}

#[derive(Debug, Serialize)]
struct Metric<T> {
    value: T,
    #[serde(skip_serializing_if = "Option::is_none")]
    lower_value: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    upper_value: Option<T>,
}

impl<T> Metric<T> {
    fn simple(value: T) -> Self {
        Self {
            value,
            lower_value: None,
            upper_value: None,
        }
    }
}

/// Aggregated benchmark metrics in BMF format
#[derive(Debug, Serialize)]
struct PipelineMetrics {
    throughput: Metric<i64>,
    memory_bytes: Metric<i64>,
    storage_bytes: Metric<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    state_amplification: Option<Metric<f64>>,
    uptime_msecs: Metric<i64>,
}

impl PipelineMetrics {
    fn new(metrics: Vec<RawMetrics>) -> Self {
        if metrics.is_empty() {
            eprintln!("No measurements were recorded? Maybe try to increase `duration`.");
            std::process::exit(1);
        }

        // Check if any metrics had input errors
        if metrics.iter().any(|m| m.input_errors) {
            eprintln!("Detected errors in input connectors during result processing, aborting.");
            std::process::exit(1);
        }

        // Calculate throughput (records per second)
        let last = metrics.last().unwrap();
        let throughput =
            (last.total_processed_records as f64 / (last.uptime_msecs as f64 / 1000.0)) as i64;

        // Calculate memory metrics
        let memory_bytes = Metric {
            value: metrics
                .iter()
                .map(|m| m.rss_bytes)
                .max_by(|a, b| a.cmp(b))
                .unwrap(),
            lower_value: Some(
                metrics
                    .iter()
                    .map(|m| m.rss_bytes)
                    .min_by(|a, b| a.cmp(b))
                    .unwrap(),
            ),
            upper_value: None,
        };

        let storage_bytes = Metric {
            value: metrics
                .iter()
                .map(|m| m.storage_bytes)
                .max_by(|a, b| a.cmp(b))
                .unwrap(),
            lower_value: Some(
                metrics
                    .iter()
                    .map(|m| m.storage_bytes)
                    .min_by(|a, b| a.cmp(b))
                    .unwrap(),
            ),
            upper_value: None,
        };

        // Calculate input bytes for state amplification
        let input_bytes = metrics.iter().last().map(|m| m.input_bytes).unwrap();

        // Calculate state amplification (storage_bytes / input_bytes)
        let state_amplification = if input_bytes > 0 {
            Some(Metric::simple(
                storage_bytes.value as f64 / input_bytes as f64,
            ))
        } else {
            None
        };

        Self {
            throughput: Metric::simple(throughput),
            memory_bytes,
            storage_bytes,
            state_amplification,
            uptime_msecs: Metric::simple(last.uptime_msecs),
        }
    }
}

/// A benchmark in BMF form
#[derive(Debug, Serialize)]
struct Benchmark {
    name: String,
    #[serde(flatten)]
    metrics: PipelineMetrics,
}

impl Benchmark {
    fn new(name: String, metrics: PipelineMetrics) -> Self {
        Self { name, metrics }
    }

    fn as_map(&self) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert(
            self.name.clone(),
            serde_json::to_value(&self.metrics).unwrap(),
        );
        map
    }

    fn format_as_text(&self) -> String {
        let mut rows = vec![];
        rows.push([
            "Metric".to_string(),
            "Value".to_string(),
            "Lower".to_string(),
            "Upper".to_string(),
        ]);

        // Add throughput
        rows.push([
            "Throughput (records/s)".to_string(),
            format!("{}", self.metrics.throughput.value),
            "-".to_string(),
            "-".to_string(),
        ]);

        // Add memory bytes
        rows.push([
            "Memory".to_string(),
            human_readable_bytes(self.metrics.memory_bytes.value),
            human_readable_bytes(self.metrics.memory_bytes.lower_value.unwrap()),
            human_readable_bytes(self.metrics.memory_bytes.upper_value.unwrap()),
        ]);

        // Add storage bytes
        rows.push([
            "Storage".to_string(),
            human_readable_bytes(self.metrics.storage_bytes.value),
            human_readable_bytes(self.metrics.storage_bytes.lower_value.unwrap()),
            human_readable_bytes(self.metrics.storage_bytes.upper_value.unwrap()),
        ]);

        // Add uptime
        rows.push([
            "Uptime (ms)".to_string(),
            self.metrics.uptime_msecs.value.to_string(),
            "-".to_string(),
            "-".to_string(),
        ]);

        // Add state amplification if present
        if let Some(amp) = &self.metrics.state_amplification {
            rows.push([
                "State Amplification".to_string(),
                format!("{:.2}", amp.value),
                "-".to_string(),
                "-".to_string(),
            ]);
        }

        format!(
            "Benchmark Results:\n{}",
            Builder::from_iter(rows).build().with(Style::rounded())
        )
    }

    fn format(&self, format: OutputFormat) -> String {
        match format {
            OutputFormat::Json => serde_json::to_string_pretty(&self.as_map()).unwrap(),
            OutputFormat::Text => self.format_as_text(),
            OutputFormat::ArrowIpc | OutputFormat::Parquet => {
                warn!("Format '{}' is not supported for benchmark results, falling back to text format", format);
                self.format_as_text()
            }
        }
    }
}

/// Collect metrics from a pipeline over time.
///
/// Returns a vector of collected metrics and whether the pipeline completed.
async fn collect_metrics(
    client: &Client,
    pipeline_name: &str,
    duration_secs: Option<u64>,
) -> Vec<Map<String, Value>> {
    let mut metrics = Vec::new();
    let start_time = Instant::now();
    let duration = duration_secs.map(Duration::from_secs);

    loop {
        sleep(Duration::from_secs(1)).await;
        match client
            .get_pipeline_stats()
            .pipeline_name(pipeline_name.to_string())
            .send()
            .await
        {
            Ok(stats) => {
                let stats = stats.into_inner();
                metrics.push(stats.clone());
                info!("Collected metrics at {}s", start_time.elapsed().as_secs());

                // Check if pipeline is complete
                if let Some(global_metrics) = stats.get("global_metrics") {
                    if let Some(complete) = global_metrics.get("pipeline_complete") {
                        if complete.as_bool().unwrap_or(false) {
                            println!("Pipeline completed, stopping benchmark");
                            break;
                        }
                    }
                }

                // Check if we've reached the duration limit
                if let Some(duration) = duration {
                    if start_time.elapsed() >= duration {
                        println!("Reached duration limit of {}s", duration_secs.unwrap());
                        break;
                    }
                }
            }
            Err(e) => {
                eprintln!("Failed to collect metrics: {}", e);
                std::process::exit(1);
            }
        }
    }

    metrics
}

/// Transform metrics into Bencher Metric Format (BMF)
fn transform_to_bmf(name: String, metrics: Vec<Map<String, Value>>) -> Benchmark {
    // Convert raw JSON metrics to structured data
    let raw_metrics: Vec<RawMetrics> = metrics.iter().map(RawMetrics::from).collect();

    // Verify incarnation_uuid is consistent
    if let Some(first) = raw_metrics.first() {
        for metric in &raw_metrics {
            if metric.incarnation_uuid != first.incarnation_uuid {
                error!("Inconsistent incarnation_uuid detected during benchmark, did the program restart while measuring?");
                std::process::exit(1);
            }
        }
    }

    Benchmark::new(name, PipelineMetrics::new(raw_metrics))
}

/// Benchmark the performance of a pipeline.
pub(crate) async fn bench(
    client: Client,
    format: OutputFormat,
    pipeline_name: String,
    duration_secs: Option<u64>,
    no_recompile: bool,
) {
    println!("Benchmarking {pipeline_name}");
    let recompile = !no_recompile;

    // Check compilation profile and storage configuration
    let pipeline = match client
        .get_pipeline()
        .pipeline_name(pipeline_name.clone())
        .send()
        .await
    {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Failed to get pipeline status: {}", e);
            std::process::exit(1);
        }
    };

    // Check compilation profile
    let compilation_profile = pipeline
        .as_ref()
        .program_config
        .as_ref()
        .and_then(|cp| cp.profile)
        .unwrap_or(CompilationProfile::Optimized);
    if compilation_profile != CompilationProfile::Optimized {
        warn!(
                "Compilation profile was set to `{:?}`. This is most likely not what you want to benchmark with. Set it to `optimized` for best performance.",
                compilation_profile
            );
    }

    // Check storage configuration
    if let Some(runtime_config) = &pipeline.runtime_config {
        if runtime_config.storage.is_none() {
            warn!(
                "Storage is not enabled for this pipeline. This may affect benchmark performance."
            );
        }
    }

    // Stop pipeline if running and restart with recompile
    let _ = Box::pin(crate::pipeline(
        OutputFormat::Text,
        PipelineAction::Shutdown {
            name: pipeline_name.clone(),
            no_wait: false,
        },
        client.clone(),
    ))
    .await;

    // Restart the pipeline with recompile if requested
    let _ = Box::pin(crate::pipeline(
        OutputFormat::Text,
        PipelineAction::Start {
            name: pipeline_name.clone(),
            recompile,
            no_wait: false,
        },
        client.clone(),
    ))
    .await;

    // Collect metrics over time
    println!("Collecting metrics...");
    let metrics = collect_metrics(&client, &pipeline_name, duration_secs).await;

    // Shutdown the pipeline after collecting metrics
    let _ = Box::pin(crate::pipeline(
        OutputFormat::Text,
        PipelineAction::Shutdown {
            name: pipeline_name.clone(),
            no_wait: true,
        },
        client.clone(),
    ))
    .await;

    let bmf_benchmark = transform_to_bmf(pipeline_name, metrics);
    println!("{}", bmf_benchmark.format(format));
}
