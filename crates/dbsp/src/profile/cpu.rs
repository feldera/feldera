//! Built-in CPU profiler.

// TODOs:
// - Richer profiling information (e.g., time distribution histogram).
// - Ability to enable/disable profiler at runtime.
// - We currently do not measure the time spent in `clock_start`/`clock_end`
//   events, which can in theory do non-trivial work.

use crate::circuit::{GlobalNodeId, RootCircuit, ThreadCpuTime, trace::SchedulerEvent};
use hashbrown::HashMap;
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

/// Why a worker's runtime is parked.
///
/// The park hook can see *that* the runtime has nothing to run, never why.  A
/// site that is about to block declares the reason with [`ParkingFor`] and the
/// hook reads it back, which turns one number into a breakdown.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub enum ParkReason {
    /// No site declared a reason.  The runtime ran out of work between steps,
    /// or inside machinery that this instrumentation does not cover.
    Unattributed = 0,
    /// A spine is waiting for its mergers to bring the batch count back below
    /// the backpressure threshold.
    MergeBackpressure = 1,
    /// An exchange or a broadcast is waiting for the other workers.
    Peers = 2,
    /// An operator task is in flight and awaiting something that none of the
    /// reasons above declared, such as background I/O.
    OperatorPending = 3,
    /// No operator task is running: the scheduler is waiting for an
    /// asynchronous operator to signal that it can run at all.
    Scheduler = 4,
}

impl ParkReason {
    pub const COUNT: usize = 5;

    /// Every reason, in the order a breakdown indexes them.
    pub const ALL: [Self; Self::COUNT] = [
        Self::Unattributed,
        Self::MergeBackpressure,
        Self::Peers,
        Self::OperatorPending,
        Self::Scheduler,
    ];

    /// Which reason wins when several are live at once.
    ///
    /// Guards nest: an operator that blocks on backpressure does so inside the
    /// scheduler's own wait, and the inner, more specific declaration is the
    /// one worth reporting.
    const PRIORITY: [Self; Self::COUNT - 1] = [
        Self::MergeBackpressure,
        Self::Peers,
        Self::OperatorPending,
        Self::Scheduler,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::Unattributed => "unattributed",
            Self::MergeBackpressure => "merge_backpressure",
            Self::Peers => "peers",
            Self::OperatorPending => "operator_pending",
            Self::Scheduler => "scheduler",
        }
    }
}

thread_local! {
    /// How many live [`ParkingFor`] guards declare each reason.
    ///
    /// A count rather than a single value because a guard stays live across the
    /// await it covers, so a task that yields leaves its declaration standing
    /// while its siblings run.
    static PARKING_FOR: Cell<[u32; ParkReason::COUNT]> =
        const { Cell::new([0; ParkReason::COUNT]) };
}

fn current_park_reason() -> ParkReason {
    let counts = PARKING_FOR.with(|counts| counts.get());
    ParkReason::PRIORITY
        .into_iter()
        .find(|reason| counts[*reason as usize] > 0)
        .unwrap_or(ParkReason::Unattributed)
}

/// Declares why this thread is about to block, for as long as the guard lives.
///
/// Hold one across the await that blocks, not merely around the call that sets
/// it up: the runtime parks after the await returns `Pending`, and only a live
/// guard is visible then.
#[must_use = "the declaration lasts only as long as the guard"]
pub struct ParkingFor(ParkReason);

impl ParkingFor {
    pub fn new(reason: ParkReason) -> Self {
        Self::adjust(reason, 1);
        Self(reason)
    }

    fn adjust(reason: ParkReason, delta: i32) {
        PARKING_FOR.with(|counts| {
            let mut counts_value = counts.get();
            counts_value[reason as usize] =
                counts_value[reason as usize].wrapping_add_signed(delta);
            counts.set(counts_value);
        });
    }
}

impl Drop for ParkingFor {
    fn drop(&mut self) {
        Self::adjust(self.0, -1);
    }
}

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
/// Timestamps are nanoseconds measured against a fixed `base` instant rather
/// than wall-clock time, so this shares a monotonic clock with the step's own
/// duration and the two can be subtracted.
#[derive(Clone, Debug)]
pub struct RuntimeIdle {
    base: Instant,
    /// When the current park started, or 0 when the runtime is not parked.
    park_start: Arc<AtomicU64>,
    /// Total time parked.
    total: Arc<AtomicU64>,
    /// The same total, split by the reason in force when the park began.
    by_reason: Arc<[AtomicU64; ParkReason::COUNT]>,
    /// The reason the current park began under.
    park_reason: Arc<AtomicUsize>,
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
            by_reason: Arc::new([const { AtomicU64::new(0) }; ParkReason::COUNT]),
            park_reason: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn now(&self) -> u64 {
        self.base.elapsed().as_nanos() as u64
    }

    /// Called by the runtime's `on_thread_park` hook.
    pub fn park(&self) {
        self.park_reason
            .store(current_park_reason() as usize, Ordering::Release);
        self.park_start.store(self.now(), Ordering::Release);
    }

    /// Called by the runtime's `on_thread_unpark` hook.
    ///
    /// An unpark without a preceding park adds nothing, which is what happens
    /// for the unpark the runtime performs as it starts running.
    pub fn unpark(&self) {
        let start = self.park_start.swap(0, Ordering::AcqRel);
        if start != 0 {
            let parked = self.now().saturating_sub(start);
            self.total.fetch_add(parked, Ordering::Release);
            self.by_reason[self.park_reason.load(Ordering::Acquire)]
                .fetch_add(parked, Ordering::Release);
        }
    }

    /// Total time parked so far.
    pub fn total(&self) -> Duration {
        Duration::from_nanos(self.total.load(Ordering::Acquire))
    }

    /// Time parked so far under each reason, indexed by [`ParkReason`].
    pub fn by_reason(&self) -> [Duration; ParkReason::COUNT] {
        std::array::from_fn(|index| {
            Duration::from_nanos(self.by_reason[index].load(Ordering::Acquire))
        })
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

    /// The step's wait time split by what the runtime was parked for.
    pub wait_by_reason: [Duration; ParkReason::COUNT],

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
    /// The same, split by reason.
    step_start_idle_by_reason: HashMap<GlobalNodeId, [Duration; ParkReason::COUNT]>,
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
                    self.step_start_idle_by_reason
                        .insert((*circuit_id).clone(), idle.by_reason());
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

                        if let Some(before) = self.step_start_idle_by_reason.remove(*circuit_id) {
                            let now = idle.by_reason();
                            for (total, (now, before)) in circuit_profile
                                .wait_by_reason
                                .iter_mut()
                                .zip(now.iter().zip(before.iter()))
                            {
                                *total += now.saturating_sub(*before);
                            }
                        }
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

#[cfg(test)]
mod test {
    use super::{ParkReason, ParkingFor, RuntimeIdle, current_park_reason};
    use std::time::{Duration, Instant};

    fn parked_under(idle: &RuntimeIdle, reason: ParkReason) -> Duration {
        idle.by_reason()[reason as usize]
    }

    /// Burns enough time for the park that brackets it to measure above zero.
    fn spin() {
        let until = Instant::now() + Duration::from_micros(50);
        while Instant::now() < until {
            std::hint::spin_loop();
        }
    }

    #[test]
    fn no_declaration_leaves_a_park_unattributed() {
        assert_eq!(current_park_reason(), ParkReason::Unattributed);
    }

    #[test]
    fn a_declaration_lasts_only_as_long_as_its_guard() {
        {
            let _parked = ParkingFor::new(ParkReason::Peers);
            assert_eq!(current_park_reason(), ParkReason::Peers);
        }
        assert_eq!(current_park_reason(), ParkReason::Unattributed);
    }

    /// Guards nest rather than overwrite, so leaving the inner one uncovers the
    /// outer one instead of clearing the declaration.
    #[test]
    fn leaving_an_inner_declaration_uncovers_the_outer_one() {
        let _outer = ParkingFor::new(ParkReason::Scheduler);
        {
            let _inner = ParkingFor::new(ParkReason::MergeBackpressure);
            assert_eq!(current_park_reason(), ParkReason::MergeBackpressure);
        }
        assert_eq!(current_park_reason(), ParkReason::Scheduler);
    }

    /// Two tasks can sit suspended under different declarations at once.  The
    /// answer must be the more specific of the two whichever order they were
    /// entered in, which is what rules out reporting whichever task happened to
    /// be polled last.
    #[test]
    fn priority_decides_between_concurrent_declarations() {
        for reversed in [false, true] {
            let (first, second) = if reversed {
                (ParkReason::Peers, ParkReason::OperatorPending)
            } else {
                (ParkReason::OperatorPending, ParkReason::Peers)
            };
            let _first = ParkingFor::new(first);
            let _second = ParkingFor::new(second);
            assert_eq!(current_park_reason(), ParkReason::Peers);
        }
    }

    #[test]
    fn a_park_is_charged_to_the_reason_it_began_under() {
        let idle = RuntimeIdle::new();

        {
            let _parked = ParkingFor::new(ParkReason::MergeBackpressure);
            idle.park();
            spin();
        }
        // The declaration is gone by the time the runtime wakes up, which is
        // why the reason has to be latched at the park rather than the unpark.
        idle.unpark();

        assert!(parked_under(&idle, ParkReason::MergeBackpressure) > Duration::ZERO);
        assert_eq!(
            parked_under(&idle, ParkReason::Unattributed),
            Duration::ZERO
        );
        assert_eq!(
            idle.total(),
            parked_under(&idle, ParkReason::MergeBackpressure)
        );
    }

    /// Every reason has a slot of its own, and the slots add up to the total.
    #[test]
    fn the_breakdown_accounts_for_the_whole_total() {
        let idle = RuntimeIdle::new();

        for reason in ParkReason::ALL {
            let _parked = (reason != ParkReason::Unattributed).then(|| ParkingFor::new(reason));
            idle.park();
            spin();
            idle.unpark();
        }

        let by_reason = idle.by_reason();
        assert!(by_reason.iter().all(|parked| *parked > Duration::ZERO));
        assert_eq!(by_reason.iter().sum::<Duration>(), idle.total());
    }

    /// An unpark that no park preceded adds nothing, so the runtime's initial
    /// unpark does not charge the whole process start-up to a reason.
    #[test]
    fn an_unmatched_unpark_adds_nothing() {
        let idle = RuntimeIdle::new();
        idle.unpark();
        assert_eq!(idle.total(), Duration::ZERO);
        assert_eq!(idle.by_reason(), [Duration::ZERO; ParkReason::COUNT]);
    }
}
