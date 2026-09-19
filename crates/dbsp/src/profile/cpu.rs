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
        atomic::{AtomicI32, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

/// Why a worker is not running.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub enum ParkReason {
    /// Unknown reason.
    Unattributed = 0,
    /// A spine is waiting for its mergers to bring the batch count back below
    /// the backpressure threshold.
    MergeBackpressure = 1,
    /// An exchange or a broadcast is waiting for the other workers.  A wait on
    /// a peer's mailbox stays here even when that peer is on another host.
    Peers = 2,
    /// An operator task is in flight and awaiting something that none of the
    /// reasons above declared, such as background I/O.
    OperatorPending = 3,
    /// No operator task is running: the scheduler is waiting for an
    /// asynchronous operator to signal that it can run.
    Scheduler = 4,
    /// The worker is waiting for a read from a storage file: either inside `pread` itself,
    /// or for a read it handed to the blocking pool to come back.
    StorageRead = 5,
    /// The thread is inside a write system call, spilling layer-file blocks to
    /// storage.
    StorageWrite = 6,
    /// The thread is inside `fsync`.  A worker reaches this only when it
    /// commits a file itself, since the checkpoint commit phase syncs on its
    /// own thread.
    StorageSync = 7,
    /// The thread is in a file metadata-related system call: creating, opening, renaming,
    /// listing, or unlinking a file.
    StorageMetadata = 8,
    /// A streaming exchange is waiting for what it has sent to another host to
    /// drain to the wire.
    Network = 9,
}

impl ParkReason {
    pub const COUNT: usize = 10;

    /// Every reason, in the order a breakdown indexes them.
    pub const ALL: [Self; Self::COUNT] = [
        Self::Unattributed,
        Self::MergeBackpressure,
        Self::Peers,
        Self::OperatorPending,
        Self::Scheduler,
        Self::StorageRead,
        Self::StorageWrite,
        Self::StorageSync,
        Self::StorageMetadata,
        Self::Network,
    ];

    /// Which reason wins when several are live at once.
    ///
    /// Guards nest: an operator that blocks on backpressure does so inside the
    /// scheduler's own wait, and the inner, more specific declaration is the
    /// one worth reporting.
    const PRIORITY: [Self; Self::COUNT - 1] = [
        Self::StorageRead,
        Self::StorageWrite,
        Self::StorageSync,
        Self::StorageMetadata,
        Self::MergeBackpressure,
        Self::Network,
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
            Self::StorageRead => "storage_read",
            Self::StorageWrite => "storage_write",
            Self::StorageSync => "storage_sync",
            Self::StorageMetadata => "storage_metadata",
            Self::Network => "network",
        }
    }
}

thread_local! {
    /// How many live [`ParkingFor`] guards declare each reason on this thread.
    ///
    /// A count rather than a single value because a guard stays live across the
    /// await it covers, so a task that yields leaves its declaration standing
    /// while its siblings run.
    ///
    /// Shared with the guards themselves: a future that holds one can be
    /// resumed on another thread of a multi-threaded runtime, as the exchange's
    /// server-side delivery is, and the guard has to give back the count it
    /// took rather than the one belonging to whichever thread dropped it.
    static PARKING_FOR: Arc<ParkDeclarations> = Arc::new(ParkDeclarations::new());

    /// How many [`BlockingFor`] guards are live on this thread.
    ///
    /// Only the outermost charges: an inner guard covers part of the interval
    /// the outer one already measures, and charging both would count that part
    /// twice, in the breakdown and in the total alike.
    /// Only a depth of zero makes a guard the outermost one, so a decrement
    /// that finds zero stays there: wrapping past it would leave every later
    /// guard looking nested, and nothing would be charged again.
    static BLOCKING_DEPTH: Cell<u32> = const { Cell::new(0) };

    /// The accumulator that [`BlockingFor`] guards on this thread charge.
    ///
    /// Set on a worker thread by the circuit that owns its runtime, so a
    /// worker's own system calls land in its own breakdown, and left unset on
    /// merger and blocking-pool threads, whose calls belong to no step.
    static RUNTIME_IDLE: RefCell<Option<RuntimeIdle>> = const { RefCell::new(None) };
}

/// The accumulator installed on this thread, if it is a worker's.
#[cfg(test)]
pub(crate) fn current_runtime_idle() -> Option<RuntimeIdle> {
    RUNTIME_IDLE.with(|idle| idle.borrow().clone())
}

/// The reasons declared on one thread, by [`ParkingFor`] guards it handed out.
///
/// Signed so that a count can only ever be wrong in the direction of saying
/// nothing.  A guard takes one and gives back the one it took, so a count
/// cannot fall below zero; if one ever did, an unsigned count would wrap to
/// `u32::MAX` and that thread would declare the reason for every park it took
/// from then on, while a negative one reads as no declaration at all.
struct ParkDeclarations([AtomicI32; ParkReason::COUNT]);

impl ParkDeclarations {
    fn new() -> Self {
        Self(std::array::from_fn(|_| AtomicI32::new(0)))
    }

    fn count(&self, reason: ParkReason) -> i32 {
        self.0[reason as usize].load(Ordering::Relaxed)
    }
}

pub(crate) fn current_park_reason() -> ParkReason {
    PARKING_FOR.with(|declared| {
        ParkReason::PRIORITY
            .into_iter()
            .find(|reason| declared.count(*reason) > 0)
            .unwrap_or(ParkReason::Unattributed)
    })
}

/// Declares why this thread is about to block, for as long as the guard lives.
///
/// Hold one across the await that blocks, not merely around the call that sets
/// it up: the runtime parks after the await returns `Pending`, and only a live
/// guard is visible then.
///
/// A guard counts against the thread that made it, and gives that count back
/// wherever it is dropped.  That matters because a future holding one can be
/// resumed on another thread: `Exchange::received`, which the shared
/// multi-threaded runtime polls, holds one across an await.  Counting the
/// thread that happens to drop it would leave the declaring thread declaring
/// the reason forever, and the other one below zero.
#[must_use = "the declaration lasts only as long as the guard"]
pub struct ParkingFor {
    reason: ParkReason,
    /// The declarations of the thread that made this guard, which are the ones
    /// it gives back, wherever it is dropped.
    declared: Arc<ParkDeclarations>,
}

impl ParkingFor {
    pub fn new(reason: ParkReason) -> Self {
        let declared = PARKING_FOR.with(Arc::clone);
        declared.0[reason as usize].fetch_add(1, Ordering::Relaxed);
        Self { reason, declared }
    }
}

impl Drop for ParkingFor {
    fn drop(&mut self) {
        let was = self.declared.0[self.reason as usize].fetch_sub(1, Ordering::Relaxed);
        debug_assert!(was > 0, "{} was given back twice", self.reason.name());
    }
}

/// Charges the time this thread spends blocked in a system call to a reason.
///
/// [`ParkingFor`] labels a park the runtime is about to take.  This covers a
/// call that blocks the thread outright, `pread` or `fsync`, during which the
/// runtime believes it is running: without it that time shows up nowhere, not
/// as CPU and not as a park.  Hold the guard across the call and nothing else.
/// Off a worker thread it costs one thread-local read and charges nothing, and
/// so does a guard nested inside another.
///
/// Only the part of the call that the thread spends off the CPU is charged.  A
/// system call is not idle time by itself: a buffered write, or a read the page
/// cache serves, runs in the kernel on this thread, where
/// `CLOCK_THREAD_CPUTIME_ID` counts it, so it already reaches
/// `circuit_cpu_time_seconds`.  Charging the whole call would put one interval
/// into both numbers, which are meant to divide the step's wall clock between
/// them.
#[must_use = "the time is charged when the guard drops"]
pub struct BlockingFor {
    reason: ParkReason,
    /// The wall and thread-CPU clocks when the call began, or `None` when
    /// nothing can be charged, off a worker thread or under another guard,
    /// which also skips the clock reads.
    start: Option<(Instant, ThreadCpuTime)>,
}

impl BlockingFor {
    pub fn new(reason: ParkReason) -> Self {
        let outermost = BLOCKING_DEPTH.with(|depth| {
            let depth_before = depth.get();
            depth.set(depth_before + 1);
            depth_before == 0
        });
        let start = (outermost && RUNTIME_IDLE.with(|idle| idle.borrow().is_some()))
            .then(|| (Instant::now(), ThreadCpuTime::now()));
        Self { reason, start }
    }
}

impl Drop for BlockingFor {
    fn drop(&mut self) {
        BLOCKING_DEPTH.with(|depth| {
            debug_assert!(depth.get() > 0, "a blocking guard was dropped twice");
            depth.set(depth.get().saturating_sub(1));
        });
        if let Some((start, cpu)) = &self.start {
            let blocked = start.elapsed().saturating_sub(cpu.elapsed());
            RUNTIME_IDLE.with(|idle| {
                if let Some(idle) = idle.borrow().as_ref() {
                    idle.blocked(self.reason, blocked);
                }
            });
        }
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

    /// Charges `blocked` under `reason` without a park.
    ///
    /// The thread spent it inside a system call, which the runtime's hooks
    /// never see; see [`BlockingFor`].
    pub fn blocked(&self, reason: ParkReason, blocked: Duration) {
        let nanos = blocked.as_nanos() as u64;
        self.total.fetch_add(nanos, Ordering::Release);
        self.by_reason[reason as usize].fetch_add(nanos, Ordering::Release);
    }

    /// Makes this the accumulator that [`BlockingFor`] guards on the current
    /// thread charge.  The circuit calls it on the worker thread that owns the
    /// runtime.
    pub fn install_on_this_thread(&self) {
        RUNTIME_IDLE.with(|idle| *idle.borrow_mut() = Some(self.clone()));
    }

    /// Total time parked or blocked so far.
    pub fn total(&self) -> Duration {
        Duration::from_nanos(self.total.load(Ordering::Acquire))
    }

    /// Time parked or blocked so far under each reason, indexed by
    /// [`ParkReason`].
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
    use super::{
        BlockingFor, ParkReason, ParkingFor, RuntimeIdle, ThreadCpuTime, current_park_reason,
        current_runtime_idle,
    };
    use std::time::{Duration, Instant};

    fn parked_under(idle: &RuntimeIdle, reason: ParkReason) -> Duration {
        idle.by_reason()[reason as usize]
    }

    /// Sleeps long enough for a guard that brackets it to measure the thread
    /// off the CPU.
    fn nap() {
        std::thread::sleep(Duration::from_millis(20));
    }

    /// Burns enough time for the park that brackets it to measure above zero.
    fn spin() {
        let until = Instant::now() + Duration::from_micros(50);
        while Instant::now() < until {
            std::hint::spin_loop();
        }
    }

    /// A breakdown maps a slot to a name through `ALL`, which is indexed by
    /// discriminant.  A reason added out of order would label another one's
    /// time.
    #[test]
    fn every_reason_sits_at_its_own_index() {
        for (index, reason) in ParkReason::ALL.into_iter().enumerate() {
            assert_eq!(reason as usize, index, "{}", reason.name());
        }
    }

    /// A reason left out of `PRIORITY` never wins, so its bucket stays empty
    /// however often a site declares it.
    #[test]
    fn every_reason_but_the_default_can_win() {
        let mut ranked = ParkReason::PRIORITY.to_vec();
        ranked.sort_by_key(|reason| *reason as usize);
        let declarable = ParkReason::ALL
            .into_iter()
            .filter(|reason| *reason != ParkReason::Unattributed)
            .collect::<Vec<_>>();
        assert_eq!(ranked, declarable);
    }

    /// A future holding a guard can be resumed on another thread of a
    /// multi-threaded runtime, which is what the exchange's server-side
    /// delivery does.  The guard gives back the count it took, so the thread
    /// that made it stops declaring the reason and the thread that dropped it
    /// never declares one at all.
    #[test]
    fn a_guard_dropped_on_another_thread_is_given_back_to_its_own() {
        let parked = ParkingFor::new(ParkReason::Peers);
        assert_eq!(current_park_reason(), ParkReason::Peers);

        std::thread::spawn(move || {
            assert_eq!(current_park_reason(), ParkReason::Unattributed);
            drop(parked);
            assert_eq!(current_park_reason(), ParkReason::Unattributed);
        })
        .join()
        .unwrap();

        assert_eq!(current_park_reason(), ParkReason::Unattributed);
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

    /// A blocking guard charges the accumulator installed on its thread, and
    /// the charge counts toward the same total the parks do, so a breakdown
    /// still adds up to it.
    #[test]
    fn a_blocking_guard_charges_its_reason() {
        let idle = RuntimeIdle::new();
        idle.install_on_this_thread();
        {
            let _blocked = BlockingFor::new(ParkReason::StorageRead);
            nap();
        }
        {
            let _parked = ParkingFor::new(ParkReason::Peers);
            idle.park();
            spin();
        }
        idle.unpark();

        assert!(parked_under(&idle, ParkReason::StorageRead) > Duration::ZERO);
        assert!(parked_under(&idle, ParkReason::Peers) > Duration::ZERO);
        assert_eq!(idle.by_reason().iter().sum::<Duration>(), idle.total());
    }

    /// A guard inside another covers time the outer one already measures, so
    /// only the outer one charges.  Counting both would inflate the wait and
    /// the breakdown together, where no invariant would catch it.
    #[test]
    fn a_nested_blocking_guard_charges_nothing() {
        let idle = RuntimeIdle::new();
        idle.install_on_this_thread();
        {
            let _outer = BlockingFor::new(ParkReason::StorageWrite);
            nap();
            {
                let _inner = BlockingFor::new(ParkReason::StorageSync);
                nap();
            }
        }

        assert!(parked_under(&idle, ParkReason::StorageWrite) > Duration::ZERO);
        assert_eq!(parked_under(&idle, ParkReason::StorageSync), Duration::ZERO);
        assert_eq!(idle.total(), parked_under(&idle, ParkReason::StorageWrite));
    }

    /// A system call that runs on the CPU, a buffered write or a cached read,
    /// is already counted in the step's CPU time.  A guard over it must charge
    /// no more than the time the thread was off the CPU, or the two numbers
    /// share an interval and the step's budget stops closing.
    #[test]
    fn a_blocking_guard_does_not_charge_time_on_the_cpu() {
        let idle = RuntimeIdle::new();
        idle.install_on_this_thread();

        let wall = Instant::now();
        let cpu = ThreadCpuTime::now();
        {
            let _blocked = BlockingFor::new(ParkReason::StorageWrite);
            for _ in 0..200 {
                spin();
            }
        }
        // Measured outside the guard, so it covers at least what the guard saw.
        // The CPU clock is read first, so that a thread descheduled between the
        // two reads widens this bound rather than narrowing it.
        let cpu = cpu.elapsed();
        let off_cpu = wall.elapsed().saturating_sub(cpu);

        assert!(
            parked_under(&idle, ParkReason::StorageWrite) <= off_cpu + Duration::from_millis(1),
            "charged {:?} of {off_cpu:?} spent off the CPU",
            parked_under(&idle, ParkReason::StorageWrite)
        );
    }

    /// Off a worker thread there is no accumulator, and the guard must cost
    /// nothing and charge nothing rather than fail.  Each test runs on its
    /// own thread, so nothing is installed here.
    #[test]
    fn a_blocking_guard_off_a_worker_charges_nothing() {
        assert!(current_runtime_idle().is_none());
        let blocked = BlockingFor::new(ParkReason::StorageSync);
        assert!(
            blocked.start.is_none(),
            "no clock is read where nothing can be charged"
        );
        spin();
        drop(blocked);
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
