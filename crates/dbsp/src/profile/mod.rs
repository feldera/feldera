//! Built-in profiling capabilities.

use crate::{
    circuit::{
        circuit_builder::Node,
        metadata::{MetaItem, OperatorMeta},
        GlobalNodeId,
    },
    monitor::TraceMonitor,
    RootCircuit,
};
use size_of::HumanBytes;
use std::{borrow::Cow, collections::HashMap, fmt::Write};

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
}

/// Runtime profiles collected from all DBSP worker threads.
pub struct DbspProfile {
    pub worker_profiles: Vec<WorkerProfile>,
}

impl crate::profile::DbspProfile {
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

    pub fn profile(&self) -> WorkerProfile {
        let mut metadata = HashMap::<GlobalNodeId, OperatorMeta>::new();

        // Make sure we add metadata for the root node.
        metadata.insert(GlobalNodeId::root(), OperatorMeta::new());

        // Collect node metadata.
        self.circuit.map_nodes_recursive(&mut |node: &dyn Node| {
            let mut meta = OperatorMeta::new();
            node.metadata(&mut meta);
            metadata.insert(node.global_id().clone(), meta);
        });

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
                ];

                for item in default_meta {
                    meta.insert(0, item);
                }
            }
        }

        WorkerProfile::new(metadata)
    }

    /// Dump profile in graphviz format.
    pub fn dump_profile(&self) -> String {
        let profile = self.profile();

        let graph = self.monitor.visualize_circuit_annotate(|node_id| {
            let mut output = String::with_capacity(1024);
            let meta = profile.metadata.get(node_id).cloned().unwrap_or_default();

            for (label, item) in meta.iter() {
                write!(output, "{label}: ").unwrap();
                item.format(&mut output).unwrap();
                output.push_str("\\l");
            }

            output
        });

        graph.to_dot()
    }
}
