//! Built-in profiling capabilities.

use crate::{
    RootCircuit, Runtime,
    circuit::{
        GlobalNodeId,
        circuit_builder::{CircuitBase, Node},
        metadata::{
            BACKGROUND_CACHE_OCCUPANCY, CIRCUIT_IDLE_TIME_SECONDS, CIRCUIT_METRICS,
            CIRCUIT_RUNTIME_ELAPSED_SECONDS, CIRCUIT_RUNTIME_SECONDS, CIRCUIT_WAIT_TIME_SECONDS,
            CircuitMetric, FOREGROUND_CACHE_OCCUPANCY, INVOCATIONS_COUNT, MetaItem, MetricId,
            MetricReading, OperatorMeta, RUNTIME_PERCENT, RUNTIME_SECONDS,
            SPINE_STORAGE_SIZE_BYTES, STEPS_COUNT, USED_MEMORY_BYTES,
        },
        runtime::ThreadType,
    },
    monitor::{TraceMonitor, visual_graph::Graph},
};
use serde::Serialize;
use size_of::HumanBytes;
use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    fmt::Write,
    fs::{self, create_dir_all},
    io::{Cursor as IoCursor, Error as IoError, Write as _},
    path::{Path, PathBuf},
    time::Duration,
};
use zip::{ZipWriter, write::SimpleFileOptions};

mod cpu;
pub use cpu::CPUProfiler;

/// Rudimentary circuit profiler.
///
/// Records circuit topology, operator metadata, and optionally CPU usage, and
/// dumps them in graphviz (dot) format.
pub struct Profiler {
    cpu_profiler: CPUProfiler,
    monitor: TraceMonitor,
    circuit: RootCircuit,
}

/// Runtime profile of an individual DBSP worker thread.
#[derive(Clone, Default, Debug, Serialize)]
pub struct WorkerProfile {
    metadata: HashMap<GlobalNodeId, OperatorMeta>,
}

impl WorkerProfile {
    fn new(metadata: HashMap<GlobalNodeId, OperatorMeta>) -> Self {
        Self { metadata }
    }

    /// Returns the profile for a specific attribute.
    ///
    /// The returned hashmap contains id's of nodes that have the specified
    /// attribute along with the value of the attribute.
    pub fn attribute_profile(&self, attr: &MetricId) -> HashMap<GlobalNodeId, MetaItem> {
        let mut result = HashMap::new();

        for (id, meta) in self.metadata.iter() {
            if let Some(item) = meta.get(attr.clone()) {
                result.insert(id.clone(), item);
            }
        }

        result
    }

    /// Returns the profile for a specific attribute of type
    /// [`MetaItem::Bytes`].
    ///
    /// Fails if the profile contains an attribute with the specified name and a
    /// type that is different from [`MetaItem::Bytes`].  On error, returns
    /// the value of the attribute that caused the failure.
    pub fn attribute_profile_as_bytes(
        &self,
        attr: &MetricId,
    ) -> Result<HashMap<GlobalNodeId, HumanBytes>, MetaItem> {
        let mut result = HashMap::new();

        for (id, meta) in self.attribute_profile(attr).into_iter() {
            if let MetaItem::Bytes(bytes) = meta {
                result.insert(id, bytes);
            } else {
                return Err(meta);
            }
        }
        Ok(result)
    }

    /// Returns the sum of values of an attribute of type [`MetaItem::Bytes`]
    /// across all nodes.
    ///
    /// Fails if the profile contains an attribute with the specified name and a
    /// type that is different from [`MetaItem::Bytes`].  On error, returns
    /// the value of the attribute that caused the failure.
    pub fn attribute_total_as_bytes(&self, attr: &MetricId) -> Result<HumanBytes, MetaItem> {
        Ok(HumanBytes::new(
            self.attribute_profile_as_bytes(attr)?
                .into_iter()
                .fold(0u64, |acc, (_, item)| acc + item.bytes),
        ))
    }

    /// Returns the total number of bytes used by all stateful operators.
    pub fn total_used_bytes(&self) -> Result<HumanBytes, MetaItem> {
        self.attribute_total_as_bytes(&USED_MEMORY_BYTES)
    }

    pub fn merge(&mut self, other: &Self) {
        for (id, dst) in self.metadata.iter_mut() {
            if let Some(src) = other.metadata.get(id) {
                dst.merge(src);
            }
        }
    }

    pub fn get_node_profile(&self, global_node_id: &GlobalNodeId) -> Option<&OperatorMeta> {
        self.metadata.get(global_node_id)
    }
}

/// Profile in graphviz format collected from all DBSP worker threads.
#[derive(Debug)]
pub struct GraphProfile {
    pub elapsed_time: Duration,

    /// Worker number of the first worker in `worker_graphs`.
    ///
    /// This will be 0 in single-host profiles.
    pub worker_offset: usize,
    pub worker_graphs: Vec<Graph>,
}

impl GraphProfile {
    const MAKEFILE: &'static str = r#"# Run `make` to easily convert the `.dot` files into PDF files for viewing.
# Run as, e.g. `make FORMATS='pdf svg png'` to convert into additional
# formats supported by `dot`.

DOTS = $(wildcard *.dot)
FORMATS = pdf

all: $(FORMATS)

define format_template
$(1): $(DOTS:.dot=.$(1))
%.$(1): %.dot
	dot -T$(1) $$< -o$$@
clean:
	rm -f $(DOTS:.dot=.$$(1))
endef

$(foreach format,$(FORMATS),$(eval $(call format_template,$(format))))

.PHONY: all clean $(FORMATS)
"#;
    /// Writes the profile as `.dot` files under `dir_path`.
    pub fn dump<P: AsRef<Path>>(&self, dir_path: P) -> Result<PathBuf, IoError> {
        let dir_path = dir_path
            .as_ref()
            .join(self.elapsed_time.as_micros().to_string());
        create_dir_all(&dir_path)?;
        for (graph, worker) in self.worker_graphs.iter().zip(self.worker_offset..) {
            fs::write(dir_path.join(format!("{worker}.dot")), graph.to_dot())?;
            fs::write(dir_path.join(format!("{worker}.txt")), graph.to_string())?;
        }
        fs::write(dir_path.join("Makefile"), Self::MAKEFILE)?;
        Ok(dir_path)
    }

    /// Returns a Zip archive containing all the profile `.dot` files.
    pub fn as_zip(&self) -> Vec<u8> {
        let mut zip = ZipWriter::new(IoCursor::new(Vec::with_capacity(65536)));
        for (graph, worker) in self.worker_graphs.iter().zip(self.worker_offset..) {
            zip.start_file(format!("{worker}.dot"), SimpleFileOptions::default())
                .unwrap();
            zip.write_all(graph.to_dot().as_bytes()).unwrap();

            zip.start_file(format!("{worker}.txt"), SimpleFileOptions::default())
                .unwrap();
            zip.write_all(graph.to_string().as_bytes()).unwrap();
        }
        zip.start_file("Makefile", SimpleFileOptions::default())
            .unwrap();
        zip.write_all(Self::MAKEFILE.as_bytes()).unwrap();
        zip.finish().unwrap().into_inner()
    }
}

/// Runtime profiles collected from all DBSP worker threads.
/// This also includes the circuit graph.
#[derive(Debug, Serialize)]
pub struct DbspProfile {
    pub metrics: &'static [CircuitMetric],
    pub worker_profiles: Vec<WorkerProfile>,
    pub graph: Option<Graph>,
}

impl DbspProfile {
    pub fn new(worker_profiles: Vec<WorkerProfile>, graph: Option<Graph>) -> Self {
        Self {
            metrics: &CIRCUIT_METRICS,
            worker_profiles,
            graph,
        }
    }

    /// Serialize the profile as a JSON string
    pub fn as_json(&self) -> String {
        serde_json::to_string(self).unwrap()
    }

    /// Encode the profile as JSON and then zip
    pub fn as_json_zip(&self) -> Vec<u8> {
        let json = self.as_json();
        let json = json.as_bytes();

        let mut zip = ZipWriter::new(std::io::Cursor::new(Vec::with_capacity(65536)));
        zip.start_file("profile.json", SimpleFileOptions::default())
            .unwrap();
        zip.write_all(json).unwrap();
        zip.finish().unwrap().into_inner()
    }

    /// Returns the sum of values of an attribute of type [`MetaItem::Bytes`]
    /// across all nodes and all worker threads.
    ///
    /// Fails if the profile contains an attribute with the specified name and a
    /// type that is different from [`MetaItem::Bytes`].  On error, returns
    /// the value of the attribute that caused the failure.
    pub fn attribute_total_as_bytes(&self, attr: &MetricId) -> Result<HumanBytes, MetaItem> {
        let mut acc = 0;

        for profile in self.worker_profiles.iter() {
            acc += profile.attribute_total_as_bytes(attr)?.bytes;
        }

        Ok(HumanBytes::new(acc))
    }

    /// Returns the total number of bytes used by all stateful operators.
    // This function is used by some Java tests, do not delete.
    pub fn total_used_bytes(&self) -> Result<HumanBytes, MetaItem> {
        self.attribute_total_as_bytes(&USED_MEMORY_BYTES)
    }

    /// Returns the total spine storage size in bytes across all operators.
    pub fn total_storage_size(&self) -> Result<HumanBytes, MetaItem> {
        self.attribute_total_as_bytes(&SPINE_STORAGE_SIZE_BYTES)
    }
}

// Public profiler API
impl Profiler {
    /// Create profiler; attach it to `circuit`.
    ///
    /// Profiler is created with CPU profiling disabled.
    pub fn new(circuit: &RootCircuit) -> Self {
        let cpu_profiler = CPUProfiler::new();

        let monitor = TraceMonitor::new_panic_on_error();
        monitor.attach_circuit_events(circuit, "monitor");

        Self {
            cpu_profiler,
            monitor,
            circuit: circuit.clone(),
        }
    }

    /// Enable CPU profiling.
    pub fn enable_cpu_profiler(&self) {
        self.cpu_profiler.attach(&self.circuit, "cpu_profiler");
    }

    pub fn profile(&self, runtime_elapsed: Duration) -> WorkerProfile {
        let mut metadata = HashMap::<GlobalNodeId, OperatorMeta>::new();

        // Collect node metadata.
        let _ = self.circuit.map_nodes_recursive(&mut |node: &dyn Node| {
            let mut meta = OperatorMeta::new();
            node.metadata(&mut meta);
            for (label, value) in node.labels().iter() {
                meta.extend([MetricReading::new(
                    MetricId(Cow::Owned(label.clone())),
                    Vec::new(),
                    MetaItem::String(value.to_string()),
                )]);
            }
            metadata.insert(node.global_id().clone(), meta);
            Ok(())
        });

        // Compute total time
        let mut total_time: Duration = Duration::default();
        for (node_id, _) in metadata.iter_mut() {
            if let Some(profile) = self.cpu_profiler.operator_profile(node_id) {
                total_time += profile.total_time();
            }
        }

        let root_meta = metadata
            .values_mut()
            .fold(OperatorMeta::new(), |mut acc, meta| {
                acc.merge(meta);
                acc
            });
        metadata.insert(GlobalNodeId::root(), root_meta);

        // Add CPU profiling info.
        for (node_id, meta) in metadata.iter_mut() {
            if let Some(profile) = self.cpu_profiler.operator_profile(node_id) {
                let default_meta = [
                    MetricReading::new(
                        INVOCATIONS_COUNT,
                        Vec::new(),
                        MetaItem::Count(profile.invocations()),
                    ),
                    MetricReading::new(
                        RUNTIME_SECONDS,
                        Vec::new(),
                        MetaItem::Duration(profile.total_time()),
                    ),
                    MetricReading::new(
                        RUNTIME_PERCENT,
                        Vec::new(),
                        MetaItem::Percent {
                            numerator: profile.total_time().as_micros() as u64,
                            denominator: total_time.as_micros() as u64,
                        },
                    ),
                ];

                meta.extend(default_meta);
            }

            // Additional metadata for circuit nodes.
            if let Some(profile) = self.cpu_profiler.circuit_profile(node_id) {
                let default_meta = metadata![
                    CIRCUIT_WAIT_TIME_SECONDS => profile.wait_profile.total_time(),
                    STEPS_COUNT => profile.step_profile.invocations(),
                    CIRCUIT_RUNTIME_SECONDS => profile.step_profile.total_time(),
                    CIRCUIT_IDLE_TIME_SECONDS => profile.idle_profile.total_time(),
                    CIRCUIT_RUNTIME_ELAPSED_SECONDS => runtime_elapsed,
                ];

                meta.extend(default_meta);

                fn cache_occupancy_metric(thread_type: ThreadType) -> MetricId {
                    match thread_type {
                        ThreadType::Foreground => FOREGROUND_CACHE_OCCUPANCY,
                        ThreadType::Background => BACKGROUND_CACHE_OCCUPANCY,
                    }
                }

                let runtime = Runtime::runtime().unwrap();
                for thread_type in [ThreadType::Foreground, ThreadType::Background] {
                    let cache =
                        runtime.get_buffer_cache(Runtime::local_worker_offset(), thread_type);
                    let (cur, max) = cache.occupancy();
                    meta.extend([MetricReading::new(
                        cache_occupancy_metric(thread_type),
                        Vec::new(),
                        MetaItem::Map(BTreeMap::from([
                            (
                                Cow::Borrowed("used"),
                                MetaItem::Bytes(HumanBytes::new(cur as u64)),
                            ),
                            (
                                Cow::Borrowed("max"),
                                MetaItem::Bytes(HumanBytes::new(max as u64)),
                            ),
                        ])),
                    )]);
                }
            }
        }

        WorkerProfile::new(metadata)
    }

    /// Dump the circuit graph without any processing.
    pub fn dump_graph(&self) -> Graph {
        self.monitor.get_circuit()
    }

    /// Dump profile in graphviz format.
    pub fn dump_profile(&self, runtime_elapsed: Duration) -> Graph {
        let profile = self.profile(runtime_elapsed);

        self.monitor.visualize_circuit_annotate(|node_id| {
            let mut output = String::with_capacity(1024);
            let meta = profile.metadata.get(node_id).cloned().unwrap_or_default();

            let mut importance = 0f64;
            for ((metric_id, labels), item) in meta.iter() {
                let label = if labels.is_empty() {
                    "".to_string()
                } else {
                    format!(
                        "[{}]",
                        labels
                            .iter()
                            .map(|(key, value)| format!("{key}={value}"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                };
                write!(output, "{metric_id}{label}: ",).unwrap();
                item.format(&mut output).unwrap();
                if metric_id == &RUNTIME_PERCENT
                    && let MetaItem::Percent {
                        numerator,
                        denominator,
                    } = item
                    && *denominator != 0
                {
                    importance = *numerator as f64 / *denominator as f64;
                };
                output.push_str("\\l");
            }

            (output, importance)
        })
    }
}
