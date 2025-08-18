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
#[derive(Clone, Default, Debug)]
pub struct OperatorCPUProfile {
    invocations: usize,
    total_time: Duration,
}

impl OperatorCPUProfile {
    pub fn add_event(&mut self, duration: Duration) {
        self.invocations += 1;
        self.total_time += duration;
    }

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

/// Circuit CPU profile.
#[derive(Clone, Default, Debug)]
pub struct CircuitCPUProfile {
    /// The number of times the circuit was blocked waiting for an async
    /// operator to become ready and the total amount of wait time.
    pub wait_profile: OperatorCPUProfile,

    /// The total number of steps performed by the circuit and the total
    /// time spent between `StepStart` and `StepEnd`.
    pub step_profile: OperatorCPUProfile,

    /// Idle periods when the circuit is not performing a step.
    ///
    /// There are two sources of idle time:
    /// - The local circuit waiting for other workers to complete a step.
    /// - The entire multithreaded circuit waiting for the client to trigger a step.
    pub idle_profile: OperatorCPUProfile,
}

#[derive(Default, Debug)]
struct CPUProfilerInner {
    start_times: HashMap<GlobalNodeId, Instant>,
    operators: HashMap<GlobalNodeId, OperatorCPUProfile>,
    wait_start_times: HashMap<GlobalNodeId, Instant>,
    step_start_times: HashMap<GlobalNodeId, Instant>,
    step_end_times: HashMap<GlobalNodeId, Instant>,
    circuit_profiles: HashMap<GlobalNodeId, CircuitCPUProfile>,
}

impl CPUProfilerInner {
    fn scheduler_event(&mut self, event: &SchedulerEvent) {
        match event {
            SchedulerEvent::StepStart { circuit_id } => {
                if let Some(end_time) = self.step_end_times.remove(*circuit_id) {
                    let duration = Instant::now().duration_since(end_time);
                    let circuit_profile = self
                        .circuit_profiles
                        .entry((*circuit_id).clone())
                        .or_insert_with(Default::default);
                    circuit_profile.idle_profile.add_event(duration);
                };

                self.step_start_times
                    .insert((*circuit_id).clone(), Instant::now());
            }
            SchedulerEvent::StepEnd { circuit_id } => {
                if let Some(start_time) = self.step_start_times.remove(*circuit_id) {
                    let duration = Instant::now().duration_since(start_time);
                    let circuit_profile = self
                        .circuit_profiles
                        .entry((*circuit_id).clone())
                        .or_insert_with(Default::default);
                    circuit_profile.step_profile.add_event(duration);
                };
                self.step_end_times
                    .insert((*circuit_id).clone(), Instant::now());
            }
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
                    op_profile.add_event(duration);
                    // println!("{}:{}:{:?}", crate::Runtime::worker_index(),
                    // node.global_id(), duration);
                };
            }
            SchedulerEvent::WaitStart { circuit_id } => {
                self.wait_start_times
                    .insert((*circuit_id).clone(), Instant::now());
            }
            SchedulerEvent::WaitEnd { circuit_id } => {
                if let Some(start_time) = self.wait_start_times.remove(*circuit_id) {
                    let duration = Instant::now().duration_since(start_time);
                    let circuit_profile = self
                        .circuit_profiles
                        .entry((*circuit_id).clone())
                        .or_insert_with(Default::default);
                    circuit_profile.wait_profile.add_event(duration);
                };
            }
            _ => (),
        }
    }
}

/// CPU profiler that attaches to a circuit and collects information about its
/// CPU utilization.
#[repr(transparent)]
#[derive(Clone, Default, Debug)]
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

    /// Returns the CPU profile of the circuit given its global node id.
    pub fn circuit_profile(&self, node: &GlobalNodeId) -> Option<CircuitCPUProfile> {
        if let Ok(this) = self.0.try_borrow() {
            this.circuit_profiles.get(node).cloned()
        } else {
            None
        }
    }
}
