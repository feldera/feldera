//! Built-in CPU profiler.

// TODOs:
// - Richer profiling information (e.g., time distribution histogram).
// - Ability to enable/disable profiler at runtime.
// - We currently do not measure the time spent in `clock_start`/`clock_end`
//   events, which can in theory do non-trivial work.

use crate::circuit::{GlobalNodeId, RootCircuit, ThreadCpuTime, trace::SchedulerEvent};
use hashbrown::HashMap;
use std::{
    cell::RefCell,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

/// Time a worker's async runtime spent with nothing to run.
///
/// The runtime that evaluates a circuit is a current-thread runtime owned by one
/// worker, so the time it spends parked is time that worker had no runnable
/// task: it is waiting for its peers at an exchange, for background work, or for
/// an asynchronous operator to become ready.
///
/// [`CPUProfiler`] samples this at step boundaries, which is what attributes the
/// idle time to a step and leaves out parks that happen outside one.
///
/// Timestamps are nanoseconds measured against [`RuntimeIdle::base`] rather than
/// wall-clock time, so this shares a monotonic clock with the step's own
/// duration and the two can be subtracted.
#[derive(Clone, Debug)]
pub struct RuntimeIdle {
    base: Instant,
    /// When the current park started, or 0 when the runtime is not parked.
    park_start: Arc<AtomicU64>,
    /// Total time parked.
    total: Arc<AtomicU64>,
}

impl Default for RuntimeIdle {
    fn default() -> Self {
        Self::new()
    }
}

impl RuntimeIdle {
    pub fn new() -> Self {
        Self {
            base: Instant::now(),
            park_start: Arc::new(AtomicU64::new(0)),
            total: Arc::new(AtomicU64::new(0)),
        }
    }

    fn now(&self) -> u64 {
        self.base.elapsed().as_nanos() as u64
    }

    /// Called by the runtime's `on_thread_park` hook.
    pub fn park(&self) {
        self.park_start.store(self.now(), Ordering::Release);
    }

    /// Called by the runtime's `on_thread_unpark` hook.
    ///
    /// An unpark without a preceding park adds nothing, which is what happens
    /// for the unpark the runtime performs as it starts running.
    pub fn unpark(&self) {
        let start = self.park_start.swap(0, Ordering::AcqRel);
        if start != 0 {
            self.total
                .fetch_add(self.now().saturating_sub(start), Ordering::Release);
        }
    }

    /// Total time parked so far.
    pub fn total(&self) -> Duration {
        Duration::from_nanos(self.total.load(Ordering::Acquire))
    }
}

/// Per-operator CPU profile.
#[derive(Clone, Default, Debug)]
pub struct OperatorCPUProfile {
    invocations: usize,
    real_time: Duration,
    cpu_time: Duration,
}

impl OperatorCPUProfile {
    pub fn add_event(&mut self, real_time: Duration, cpu_time: Duration) {
        self.invocations += 1;
        self.real_time += real_time;
        self.cpu_time += cpu_time;
    }

    /// Returns the number of times the operator has been invoked.
    /// This number is the same for all operators in a synchronous
    /// circuit.
    pub fn invocations(&self) -> usize {
        self.invocations
    }

    /// Total elapsed time spent evaluating the operator across all invocations.
    pub fn real_time(&self) -> Duration {
        self.real_time
    }

    /// Total CPU time spent evaluating the operator across all invocations.
    pub fn cpu_time(&self) -> Duration {
        self.cpu_time
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
    operators: HashMap<GlobalNodeId, OperatorCPUProfile>,
    step_start_times: HashMap<GlobalNodeId, Instant>,
    step_end_times: HashMap<GlobalNodeId, Instant>,
    /// Thread CPU time when the current step started, per circuit.
    step_start_cpu: HashMap<GlobalNodeId, Duration>,
    /// Runtime idle total when the current step started, per circuit.
    step_start_idle: HashMap<GlobalNodeId, Duration>,
    circuit_profiles: HashMap<GlobalNodeId, CircuitCPUProfile>,
    /// Set when the profiler is attached; `None` leaves the wait and CPU
    /// figures at zero rather than reporting a number with nothing behind it.
    runtime_idle: Option<RuntimeIdle>,
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
                    circuit_profile
                        .idle_profile
                        .add_event(duration, Duration::ZERO);
                };

                self.step_start_times
                    .insert((*circuit_id).clone(), Instant::now());
                self.step_start_cpu
                    .insert((*circuit_id).clone(), ThreadCpuTime::now().0);
                if let Some(idle) = &self.runtime_idle {
                    self.step_start_idle
                        .insert((*circuit_id).clone(), idle.total());
                }
            }
            SchedulerEvent::StepEnd { circuit_id } => {
                if let Some(start_time) = self.step_start_times.remove(*circuit_id) {
                    let duration = Instant::now().duration_since(start_time);
                    let cpu = self
                        .step_start_cpu
                        .remove(*circuit_id)
                        .map(|start| ThreadCpuTime::now().0.saturating_sub(start))
                        .unwrap_or_default();
                    let circuit_profile = self
                        .circuit_profiles
                        .entry((*circuit_id).clone())
                        .or_insert_with(Default::default);
                    circuit_profile.step_profile.add_event(duration, cpu);

                    // Time the runtime spent parked during this step. Measured
                    // here rather than from the scheduler's `WaitStart`/`WaitEnd`
                    // events: since operators became Rust-async, a blocked
                    // operator is a pending task rather than an idle scheduler,
                    // so those events no longer fire for it.
                    if let (Some(idle), Some(before)) = (
                        self.runtime_idle.as_ref(),
                        self.step_start_idle.remove(*circuit_id),
                    ) {
                        circuit_profile
                            .wait_profile
                            .add_event(idle.total().saturating_sub(before), Duration::ZERO);
                    }
                };
                self.step_end_times
                    .insert((*circuit_id).clone(), Instant::now());
            }
            SchedulerEvent::EvalStart { .. } => {}
            SchedulerEvent::EvalEnd { node, elapsed_time } => {
                let op_profile = self
                    .operators
                    .entry(node.global_id().clone())
                    .or_insert_with(Default::default);
                op_profile.add_event(elapsed_time.real, elapsed_time.cpu);
                // println!("{}:{}:{:?}", crate::Runtime::worker_index(),
                // node.global_id(), duration);
            }
            // `WaitStart`/`WaitEnd` are deliberately ignored: they report only
            // the scheduler having no runnable task, which the runtime's park
            // time already covers, and counting both would double count.
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
    pub fn attach(&self, circuit: &RootCircuit, handler_name: &str, runtime_idle: RuntimeIdle) {
        if let Ok(mut this) = self.0.try_borrow_mut() {
            this.runtime_idle = Some(runtime_idle);
        }
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
