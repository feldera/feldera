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
use std::{borrow::Cow, collections::HashMap, fmt::Write};

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

    /// Dump profile in graphviz format.
    pub fn dump_profile(&self) -> String {
        let mut metadata = HashMap::<GlobalNodeId, OperatorMeta>::new();

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
        }

        let graph = self.monitor.visualize_circuit_annotate(|node_id| {
            let mut output = String::with_capacity(1024);
            let meta = metadata.get(node_id).cloned().unwrap_or_default();

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
