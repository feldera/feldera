//! Built-in CPU profiler.

// TODOs:
// - Richer profiling information (e.g., time distribution histogram).
// - Ability to enable/disable profiler at runtime.
// - We currently do not measure the time spent in `clock_start`/`clock_end`
//   events, which can in theory do non-trivial work.

use crate::circuit::{trace::SchedulerEvent, GlobalNodeId, RootCircuit};
use hashbrown::HashMap;
use std::{
    cell::RefCell,
    rc::Rc,
    time::{Duration, Instant},
};

/// Per-operator CPU profile.
#[derive(Clone, Default)]
pub struct OperatorCPUProfile {
    invocations: usize,
    total_time: Duration,
}

impl OperatorCPUProfile {
    /// Returns the number of times the operator has been invoked.
    /// This number is the same for all operators in a synchronous
    /// circuit.
    pub fn invocations(&self) -> usize {
        self.invocations
    }

    /// Total time spent evaluating the operator across all invocations.
    pub fn total_time(&self) -> Duration {
        self.total_time
    }
}

#[derive(Default)]
struct CPUProfilerInner {
    start_times: HashMap<GlobalNodeId, Instant>,
    operators: HashMap<GlobalNodeId, OperatorCPUProfile>,
}

impl CPUProfilerInner {
    fn scheduler_event(&mut self, event: &SchedulerEvent) {
        match event {
            SchedulerEvent::EvalStart { node } => {
                self.start_times
                    .insert(node.global_id().clone(), Instant::now());
            }
            SchedulerEvent::EvalEnd { node } => {
                if let Some(start_time) = self.start_times.remove(node.global_id()) {
                    let duration = Instant::now().duration_since(start_time);
                    let op_profile = self
                        .operators
                        .entry(node.global_id().clone())
                        .or_insert_with(Default::default);
                    op_profile.invocations += 1;
                    op_profile.total_time += duration;
                };
            }
            _ => (),
        }
    }
}

/// CPU profiler that attaches to a circuit and collects information about its
/// CPU utilization.
#[repr(transparent)]
#[derive(Clone, Default)]
pub struct CPUProfiler(Rc<RefCell<CPUProfilerInner>>);

impl CPUProfiler {
    /// Create a new CPU profiler instance.
    pub fn new() -> Self {
        Self::default()
    }

    /// Attach CPU profiler to a circuit.  The profiler will start measuring
    /// circuit's CPU usage.
    pub fn attach(&self, circuit: &RootCircuit, handler_name: &str) {
        let self_clone = self.clone();

        circuit.register_scheduler_event_handler(handler_name, move |event| {
            if let Ok(mut this) = self_clone.0.try_borrow_mut() {
                this.scheduler_event(event);
            };
        });
    }

    /// Returns CPU usage information of the specified circuit node (operator)
    /// or subcircuit or `None` if the profiler has not observed any
    /// activations of the specified node.
    pub fn operator_profile(&self, node: &GlobalNodeId) -> Option<OperatorCPUProfile> {
        if let Ok(this) = self.0.try_borrow() {
            this.operators.get(node).cloned()
        } else {
            None
        }
    }
}
