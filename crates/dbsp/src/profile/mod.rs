//! Built-in profiling capabilities.

use crate::{
    circuit::{
        circuit_builder::Node,
        metadata::{MetaItem, OperatorMeta},
        runtime::ThreadType,
        GlobalNodeId,
    },
    monitor::{visual_graph::Graph, TraceMonitor},
    RootCircuit, Runtime,
};
use size_of::HumanBytes;
use std::{
    borrow::Cow,
    collections::HashMap,
    fmt::Write,
    fs::{self, create_dir_all},
    io::{Cursor as IoCursor, Error as IoError, Write as _},
    path::{Path, PathBuf},
    time::Duration,
};
use zip::{write::FileOptions, ZipWriter};

mod cpu;
use crate::circuit::metadata::{
    ALLOCATED_BYTES_LABEL, NUM_ENTRIES_LABEL, SHARED_BYTES_LABEL, USED_BYTES_LABEL,
};
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
#[derive(Clone, Default, Debug)]
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
    pub fn attribute_profile(&self, attr: &str) -> HashMap<GlobalNodeId, MetaItem> {
        let mut result = HashMap::new();

        for (id, meta) in self.metadata.iter() {
            if let Some(item) = meta.get(attr) {
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
        attr: &str,
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
    pub fn attribute_total_as_bytes(&self, attr: &str) -> Result<HumanBytes, MetaItem> {
        Ok(HumanBytes::new(
            self.attribute_profile_as_bytes(attr)?
                .into_iter()
                .fold(0u64, |acc, (_, item)| acc + item.bytes),
        ))
    }

    /// Returns the profile for a specific attribute of type [`MetaItem::Int`].
    ///
    /// Fails if the profile contains an attribute with the specified name and a
    /// type that is different from [`MetaItem::Int`].  On error, returns
    /// the value of the attribute that caused the failure.
    pub fn attribute_profile_as_int(
        &self,
        attr: &str,
    ) -> Result<HashMap<GlobalNodeId, usize>, MetaItem> {
        let mut result = HashMap::new();

        for (id, meta) in self.attribute_profile(attr).into_iter() {
            if let MetaItem::Int(val) = meta {
                result.insert(id, val);
            } else {
                return Err(meta);
            }
        }
        Ok(result)
    }

    /// Returns the sum of values of an attribute of type [`MetaItem::Int`]
    /// across all nodes.
    ///
    /// Fails if the profile contains an attribute with the specified name and a
    /// type that is different from [`MetaItem::Int`].  On error, returns
    /// the value of the attribute that caused the failure.
    pub fn attribute_total_as_int(&self, attr: &str) -> Result<usize, MetaItem> {
        Ok(self
            .attribute_profile_as_int(attr)?
            .into_iter()
            .fold(0usize, |acc, (_, item)| acc + item))
    }

    /// Returns the number of entries stored in each stateful operator.
    pub fn relation_size_profile(&self) -> Result<HashMap<GlobalNodeId, usize>, MetaItem> {
        self.attribute_profile_as_int(NUM_ENTRIES_LABEL)
    }

    /// Returns the total number of entries across all stateful operators.
    pub fn total_relation_size(&self) -> Result<usize, MetaItem> {
        self.attribute_total_as_int(NUM_ENTRIES_LABEL)
    }

    /// Returns the number of used bytes for each stateful operator.
    pub fn used_bytes_profile(&self) -> Result<HashMap<GlobalNodeId, HumanBytes>, MetaItem> {
        self.attribute_profile_as_bytes(USED_BYTES_LABEL)
    }

    /// Returns the total number of bytes used by all stateful operators.
    pub fn total_used_bytes(&self) -> Result<HumanBytes, MetaItem> {
        self.attribute_total_as_bytes(USED_BYTES_LABEL)
    }

    /// Returns the number of allocated bytes for each stateful operator.
    pub fn allocated_bytes_profile(&self) -> Result<HashMap<GlobalNodeId, HumanBytes>, MetaItem> {
        self.attribute_profile_as_bytes(ALLOCATED_BYTES_LABEL)
    }

    /// Returns the total number of allocated bytes across all stateful
    /// operators.
    pub fn total_allocated_bytes(&self) -> Result<HumanBytes, MetaItem> {
        self.attribute_total_as_bytes(ALLOCATED_BYTES_LABEL)
    }

    /// Returns the number of shared bytes for each stateful operator.
    pub fn shared_bytes_profile(&self) -> Result<HashMap<GlobalNodeId, HumanBytes>, MetaItem> {
        self.attribute_profile_as_bytes(SHARED_BYTES_LABEL)
    }

    /// Returns the total number of shared bytes across all stateful operators.
    pub fn total_shared_bytes(&self) -> Result<HumanBytes, MetaItem> {
        self.attribute_total_as_bytes(SHARED_BYTES_LABEL)
    }

    pub fn merge(&mut self, other: &Self) {
        for (id, dst) in self.metadata.iter_mut() {
            if let Some(src) = other.metadata.get(id) {
                dst.merge(src);
            }
        }
    }
}

/// Profile in graphviz format collected from all DBSP worker threads.
#[derive(Debug)]
pub struct GraphProfile {
    pub elapsed_time: Duration,
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
        for (worker, graph) in self.worker_graphs.iter().enumerate() {
            fs::write(dir_path.join(format!("{worker}.dot")), graph.to_dot())?;
        }
        fs::write(dir_path.join("Makefile"), Self::MAKEFILE)?;
        Ok(dir_path)
    }

    /// Returns a Zip archive containing all the profile `.dot` files.
    pub fn as_zip(&self) -> Vec<u8> {
        let mut zip = ZipWriter::new(IoCursor::new(Vec::with_capacity(65536)));
        let elapsed = self.elapsed_time.as_micros();
        for (worker, graph) in self.worker_graphs.iter().enumerate() {
            zip.start_file(
                format!("profile/{elapsed}/{worker}.dot"),
                FileOptions::default(),
            )
            .unwrap();
            zip.write_all(graph.to_dot().as_bytes()).unwrap();
        }
        zip.start_file(
            format!("profile/{elapsed}/Makefile"),
            FileOptions::default(),
        )
        .unwrap();
        zip.write_all(Self::MAKEFILE.as_bytes()).unwrap();
        zip.finish().unwrap().into_inner()
    }
}

/// Runtime profiles collected from all DBSP worker threads.
#[derive(Debug)]
pub struct DbspProfile {
    pub worker_profiles: Vec<WorkerProfile>,
}

impl DbspProfile {
    pub fn new(worker_profiles: Vec<WorkerProfile>) -> Self {
        Self { worker_profiles }
    }

    /// Compute aggregate profile for a specific attribute across all workers.
    ///
    /// # Arguments
    ///
    /// - `attr` - attribute name
    /// - `default` - default value of the attribute
    /// - `combine` - combines a value of the attribute with a new value
    ///   retrieved from a `MetaItem`. Fails if the `MetaItem` has incorrect
    ///   type for the attribute.
    pub fn attribute_profile<T, DF, CF>(
        &self,
        attr: &str,
        default: DF,
        combine: CF,
    ) -> Result<HashMap<GlobalNodeId, T>, MetaItem>
    where
        DF: Fn() -> T,
        CF: Fn(&T, &MetaItem) -> Result<T, MetaItem>,
    {
        let mut result = HashMap::new();

        for profile in self.worker_profiles.iter() {
            let new = profile.attribute_profile(attr);
            for (id, item) in new.into_iter() {
                let entry = result.entry(id).or_insert_with(&default);
                *entry = combine(entry, &item)?;
            }
        }

        Ok(result)
    }

    /// Compute aggregate profile of an attribute of type [`MetaItem::Bytes`] by
    /// summing up the values of the attribute across all workers.
    pub fn attribute_profile_as_bytes(
        &self,
        attr: &str,
    ) -> Result<HashMap<GlobalNodeId, HumanBytes>, MetaItem> {
        self.attribute_profile(
            attr,
            || HumanBytes::new(0),
            |bytes, item| match item {
                MetaItem::Bytes(new_bytes) => Ok(HumanBytes::new(bytes.bytes + new_bytes.bytes)),
                _ => Err(item.clone()),
            },
        )
    }

    /// Returns the sum of values of an attribute of type [`MetaItem::Bytes`]
    /// across all nodes and all worker threads.
    ///
    /// Fails if the profile contains an attribute with the specified name and a
    /// type that is different from [`MetaItem::Bytes`].  On error, returns
    /// the value of the attribute that caused the failure.
    pub fn attribute_total_as_bytes(&self, attr: &str) -> Result<HumanBytes, MetaItem> {
        let mut acc = 0;

        for profile in self.worker_profiles.iter() {
            acc += profile.attribute_total_as_bytes(attr)?.bytes;
        }

        Ok(HumanBytes::new(acc))
    }

    /// Compute aggregate profile of an attribute of type [`MetaItem::Int`] by
    /// summing up the values of the attribute across all workers.
    pub fn attribute_profile_as_int(
        &self,
        attr: &str,
    ) -> Result<HashMap<GlobalNodeId, usize>, MetaItem> {
        self.attribute_profile(
            attr,
            || 0,
            |val, item| match item {
                MetaItem::Int(new_val) => Ok(val + new_val),
                _ => Err(item.clone()),
            },
        )
    }

    /// Returns the sum of values of an attribute of type [`MetaItem::Int`]
    /// across all nodes and all worker threads.
    ///
    /// Fails if the profile contains an attribute with the specified name and a
    /// type that is different from [`MetaItem::Int`].  On error, returns
    /// the value of the attribute that caused the failure.
    pub fn attribute_total_as_int(&self, attr: &str) -> Result<usize, MetaItem> {
        let mut acc = 0usize;

        for profile in self.worker_profiles.iter() {
            acc += profile.attribute_total_as_int(attr)?;
        }

        Ok(acc)
    }

    /// Returns the number of table entries stored in each stateful operator.
    pub fn relation_size_profile(&self) -> Result<HashMap<GlobalNodeId, usize>, MetaItem> {
        self.attribute_profile_as_int(NUM_ENTRIES_LABEL)
    }

    /// Returns the total number of table entries across all stateful operators.
    pub fn total_relation_size(&self) -> Result<usize, MetaItem> {
        self.attribute_total_as_int(NUM_ENTRIES_LABEL)
    }

    /// Returns the number of used bytes for each stateful operator.
    pub fn used_bytes_profile(&self) -> Result<HashMap<GlobalNodeId, HumanBytes>, MetaItem> {
        self.attribute_profile_as_bytes(USED_BYTES_LABEL)
    }

    /// Returns the total number of bytes used by all stateful operators.
    pub fn total_used_bytes(&self) -> Result<HumanBytes, MetaItem> {
        self.attribute_total_as_bytes(USED_BYTES_LABEL)
    }

    /// Returns the number of allocated bytes for each stateful operator.
    pub fn allocated_bytes_profile(&self) -> Result<HashMap<GlobalNodeId, HumanBytes>, MetaItem> {
        self.attribute_profile_as_bytes(ALLOCATED_BYTES_LABEL)
    }

    /// Returns the total number of allocated bytes across all stateful
    /// operators.
    pub fn total_allocated_bytes(&self) -> Result<HumanBytes, MetaItem> {
        self.attribute_total_as_bytes(ALLOCATED_BYTES_LABEL)
    }

    /// Returns the number of shared bytes for each stateful operator.
    pub fn shared_bytes_profile(&self) -> Result<HashMap<GlobalNodeId, HumanBytes>, MetaItem> {
        self.attribute_profile_as_bytes(SHARED_BYTES_LABEL)
    }

    /// Returns the total number of shared bytes across all stateful operators.
    pub fn total_shared_bytes(&self) -> Result<HumanBytes, MetaItem> {
        self.attribute_total_as_bytes(SHARED_BYTES_LABEL)
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
                    (
                        Cow::Borrowed("invocations"),
                        MetaItem::Int(profile.invocations()),
                    ),
                    (
                        Cow::Borrowed("time"),
                        MetaItem::Duration(profile.total_time()),
                    ),
                    (
                        Cow::Borrowed("time%"),
                        MetaItem::Percent {
                            numerator: profile.total_time().as_micros() as u64,
                            denominator: total_time.as_micros() as u64,
                        },
                    ),
                ];

                for item in default_meta {
                    meta.insert(0, item);
                }
            }

            // Additional metadata for circuit nodes.
            if let Some(profile) = self.cpu_profiler.circuit_profile(node_id) {
                let default_meta = [
                    (
                        Cow::Borrowed("wait_time"),
                        MetaItem::Duration(profile.wait_profile.total_time()),
                    ),
                    (
                        Cow::Borrowed("steps"),
                        MetaItem::Int(profile.step_profile.invocations()),
                    ),
                    (
                        Cow::Borrowed("total_runtime"),
                        MetaItem::Duration(profile.step_profile.total_time()),
                    ),
                    (
                        Cow::Borrowed("runtime_elapsed"),
                        MetaItem::Duration(runtime_elapsed),
                    ),
                ];

                for item in default_meta {
                    meta.insert(0, item);
                }

                let runtime = Runtime::runtime().unwrap();
                for thread_type in [ThreadType::Foreground, ThreadType::Background] {
                    let cache = runtime.get_buffer_cache(Runtime::worker_index(), thread_type);
                    let (cur, max) = cache.occupancy();
                    meta.insert(
                        0,
                        (
                            Cow::from(format!("{thread_type} cache occupancy")),
                            MetaItem::String(format!(
                                "{} used (max {})",
                                HumanBytes::new(cur as u64),
                                HumanBytes::new(max as u64)
                            )),
                        ),
                    );
                }
            }
        }

        WorkerProfile::new(metadata)
    }

    /// Dump profile in graphviz format.
    pub fn dump_profile(&self, runtime_elapsed: Duration) -> Graph {
        let profile = self.profile(runtime_elapsed);

        self.monitor.visualize_circuit_annotate(|node_id| {
            let mut output = String::with_capacity(1024);
            let meta = profile.metadata.get(node_id).cloned().unwrap_or_default();

            let mut importance = 0f64;
            for (label, item) in meta.iter() {
                write!(output, "{label}: ").unwrap();
                item.format(&mut output).unwrap();
                if label == "time%" {
                    if let MetaItem::Percent {
                        numerator,
                        denominator,
                    } = item
                    {
                        if *denominator != 0 {
                            importance = *numerator as f64 / *denominator as f64;
                        }
                    };
                }
                output.push_str("\\l");
            }

            (output, importance)
        })
    }
}
