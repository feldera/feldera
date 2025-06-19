//! Dynamic scheduler.
//!
//! The dynamic scheduler picks the next circuit node to evaluate dynamically
//! based on three criteria:
//! 1. A node can only be evaluated after its predecessors.
//! 2. An async node can only be evaluated in a ready state (see
//!    [`Operator::ready`](`crate::circuit::operator_traits::Operator::ready`)).
//! 3. Pick a highest priority node among nodes that satisfy the first two
//!    conditions.
//!
//! Unlike [`StaticScheduler`](`crate::circuit::schedule::StaticScheduler`),
//! the dynamic scheduler blocks when there are no runnable nodes instead of
//! busy waiting.
//!
//! # Design
//!
//! ## Tasks
//!
//! We model the scheduling problem as a set of tasks.  Each task represents a
//! node in the circuit along with some metadata.  At runtime, the scheduler
//! tracks for each task the number of predecessor nodes that are yet to be
//! scheduled, and, for async nodes, their ready status.  Once criteria 1 and 2
//! above are satisfied, the scheduler moves the task to the run queue.
//!
//! ## Run queue
//!
//! The run queue is organized as a priority queue, with the scheduler picking
//! one of the highest-priority runnable tasks to run next.  Priority assignment
//! is heuristic.
//!
//! ## Notification processing
//!
//! The scheduler relies on notifications to determine when an async operator
//! becomes ready.  In order to minimize expensive synchronization when
//! processing notifications, we adopt an approach inspired by RCU locks.  The
//! event notifier simply records each notification in a set and unparks the
//! scheduler thread. Whenever the runnable queue becomes empty, the scheduler
//! scans the ready set marking operators ready and moving them to the runnable
//! queue when necessary. If the runnable queue is still empty, the scheduler
//! thread parks itself waiting for the next ready notification.

use std::{
    cell::{RefCell, RefMut},
    collections::{BTreeSet, HashMap, HashSet},
    panic,
    sync::{Arc, Mutex},
};

use crate::circuit::{
    runtime::Runtime,
    schedule::{
        util::{circuit_graph, ownership_constraints},
        Error, Scheduler,
    },
    trace::SchedulerEvent,
    Circuit, GlobalNodeId, NodeId,
};
use petgraph::algo::toposort;
use tokio::{select, sync::Notify, task::JoinSet};

#[derive(PartialEq, Eq, Debug)]
enum FlushState {
    UnflushedDependencies(usize),
    Started,
    Completed,
}

/// A task is a unit of work scheduled by the dynamic scheduler.
/// It contains a reference to a node in the circuit and associted metadata.
struct Task {
    // Immutable fields (initialized once when preparing the scheduler).
    /// Circuit node to be scheduled.
    node_id: NodeId,

    /// The number of predecessors of the node in the circuit graph.
    /// All predecessors must be evaluated before the node can be evaluated.
    num_predecessors: usize,

    /// Successors of the node in the circuit graph.
    successors: Vec<NodeId>,

    /// `true` if this is an async node.  The node can only be evaluated in a
    /// ready state.
    is_async: bool,

    // Mutable fields reset on each microstep.
    /// Number of predecessors not yet evaluated.  Set to `num_predecessors`
    /// at the start of each step.
    unsatisfied_dependencies: usize,

    /// `true` if the async node is known to be in a ready state.  Always
    /// `true` for non-async nodes.
    is_ready: bool,

    /// Task has been scheduled (put on the run queue) in the current clock
    /// cycle.
    scheduled: bool,

    // Mutable fields reset on each macrostep.
    flush_state: FlushState,
}

/// The set of async nodes for which the scheduler has received ready
/// notifications.
#[derive(Clone)]
struct Notifications {
    /// Nodes that received notifications.
    nodes: Arc<Mutex<HashSet<NodeId>>>,

    /// Notifier to wake up the scheduler thread when a notification arrives.
    notify: Arc<Notify>,
}

impl Notifications {
    fn new(size: usize) -> Self {
        Self {
            nodes: Arc::new(Mutex::new(HashSet::with_capacity(size))),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Add a new notification.
    fn notify(&self, node_id: NodeId) {
        self.nodes.lock().unwrap().insert(node_id);
        self.notify.notify_one();
    }

    /// Wait for at least one notification.
    async fn wait(&self) {
        self.notify.notified().await
    }
}

/// Dynamic scheduler internals.
struct Inner {
    // Immutable fields (initialized once when preparing the scheduler).
    /// Tasks that must be evaluated at each clock cycle.
    tasks: HashMap<NodeId, Task>,

    // Mutable fields.
    /// Ready notifications received while the scheduler was busy or sleeping.
    notifications: Notifications,

    /// Currently running tasks. Each task returns node id along with status,
    /// so that the scheduler can match it back to the circuit node.
    handles: JoinSet<(NodeId, Result<(), Error>)>,

    /// True when the scheduler is waiting for at least one task to become ready.
    waiting: bool,

    flush: bool,

    // Reset on each macrostep
    unflushed_operators: usize,
}

impl Inner {
    /// Called when task `node_id` has completed to update unsatisfied dependencies of
    /// all successors and spawn any tasks that are ready to run.
    fn schedule_successors<C>(&mut self, circuit: &C, node_id: NodeId, flush_complete: bool)
    where
        C: Circuit,
    {
        // Don't use iterator, as we will borrow `tasks` again below.
        for i in 0..self.tasks[&node_id].successors.len() {
            let succ_id = self.tasks[&node_id].successors[i];
            debug_assert!(self.tasks.contains_key(&succ_id));
            let successor = self.tasks.get_mut(&succ_id).unwrap();
            debug_assert_ne!(successor.unsatisfied_dependencies, 0);
            successor.unsatisfied_dependencies -= 1;
            if flush_complete {
                let FlushState::UnflushedDependencies(ref mut n) = &mut successor.flush_state
                else {
                    panic!(
                        "Internal scheduler error: node {node_id} is in state {:?} while it still has unflushed dependencies",
                        successor.flush_state
                    );
                };
                debug_assert!(*n > 0);
                *n -= 1;
            }
            if successor.unsatisfied_dependencies == 0 && successor.is_ready {
                self.spawn_task(circuit, succ_id);
            }
        }
    }

    /// Process and dequeue new notifications.
    fn process_notifications<C>(&mut self, circuit: &C)
    where
        C: Circuit,
    {
        // Don't hold the mutex during iteration below to avoid reentering the lock.
        let mut nodes = std::mem::take(&mut *self.notifications.nodes.lock().unwrap());

        // False positive via rust-clippy/#8963
        #[allow(unknown_lints)]
        #[allow(clippy::significant_drop_in_scrutinee)]
        for id in nodes.drain() {
            let task = self.tasks.get_mut(&id).unwrap();
            debug_assert!(task.is_async);

            // Ignore duplicate notifications.
            if task.is_ready {
                continue;
            }

            // Ignore spurious notifications.
            if circuit.ready(id) {
                task.is_ready = true;

                let node_id = task.node_id;

                // We can see a notification for an already scheduled task
                // indicating that it's become ready again.
                // This notification should take effect at the next clock
                // cycle.
                if task.unsatisfied_dependencies == 0 && !task.scheduled {
                    self.spawn_task(circuit, node_id);
                }
            }
        }
    }

    fn prepare<C>(circuit: &C, nodes: Option<&BTreeSet<NodeId>>) -> Result<Self, Error>
    where
        C: Circuit,
    {
        let nodes = nodes
            .map(|nodes| nodes.iter().cloned().collect::<BTreeSet<_>>())
            .unwrap_or_else(|| BTreeSet::from_iter(circuit.node_ids()));

        // Check that ownership constraints don't introduce cycles.
        let mut g: petgraph::prelude::GraphMap<NodeId, (), petgraph::Directed> =
            circuit_graph(circuit);

        // println!("g: {g:#?}");

        let extra_constraints = ownership_constraints(circuit)?;

        for (from, to) in extra_constraints.iter() {
            g.add_edge(*from, *to, ());
        }

        // `toposort` fails if the graph contains cycles.
        toposort(&g, None).map_err(|e| Error::CyclicCircuit {
            node_id: GlobalNodeId::child_of(circuit, e.node_id()),
        })?;

        let num_nodes = nodes.len();
        let mut successors: HashMap<NodeId, Vec<NodeId>> = HashMap::with_capacity(num_nodes);
        let mut predecessors: HashMap<NodeId, Vec<NodeId>> = HashMap::with_capacity(num_nodes);
        circuit.edges().iter().for_each(|edge| {
            if let Some(stream) = &edge.stream {
                // println!("clearing consumer count for stream {}", stream.stream_id());
                stream.clear_consumer_count();
            }
        });

        for edge in circuit.edges().iter() {
            if nodes.contains(&edge.to) && nodes.contains(&edge.from) {
                successors.entry(edge.from).or_default().push(edge.to);

                predecessors.entry(edge.to).or_default().push(edge.from);
                if let Some(stream) = &edge.stream {
                    // println!(
                    //     "Registering {} as consumer for stream {}",
                    //     edge.to,
                    //     stream.stream_id()
                    // );
                    stream.register_consumer();
                }
            }
        }

        // Add ownership constraints to the graph.
        for (from, to) in extra_constraints.into_iter() {
            if nodes.contains(&to) && nodes.contains(&from) {
                successors.entry(from).or_default().push(to);
                predecessors.entry(to).or_default().push(from);
            }
        }

        let mut tasks = HashMap::new();
        let mut num_async_nodes = 0;

        for &node_id in nodes.iter() {
            let num_predecessors = predecessors.entry(node_id).or_default().len();

            let is_async = circuit.is_async_node(node_id);
            if is_async {
                num_async_nodes += 1;
            }

            tasks.insert(
                node_id,
                Task {
                    node_id,
                    num_predecessors,
                    successors: successors.entry(node_id).or_default().clone(),
                    is_async,
                    unsatisfied_dependencies: num_predecessors,
                    is_ready: !is_async,
                    scheduled: false,
                    flush_state: FlushState::UnflushedDependencies(num_predecessors),
                },
            );
        }

        let scheduler = Self {
            tasks,
            notifications: Notifications::new(num_async_nodes),
            handles: JoinSet::new(),
            waiting: false,
            flush: false,
            unflushed_operators: nodes.len(),
        };

        // Setup scheduler callbacks.
        for &node_id in nodes.iter() {
            if circuit.is_async_node(node_id) {
                let notifications = scheduler.notifications.clone();
                circuit.register_ready_callback(
                    node_id,
                    Box::new(move || notifications.notify(node_id)),
                );

                // Since we missed any earlier notifications, generate one for
                // each ready node.
                if circuit.ready(node_id) {
                    scheduler.notifications.notify(node_id);
                }
            }
        }

        Ok(scheduler)
    }

    /// Spawn a tokio task to evaluate `node_id`.
    fn spawn_task<C>(&mut self, circuit: &C, node_id: NodeId)
    where
        C: Circuit,
    {
        let task = self.tasks.get_mut(&node_id).unwrap();
        debug_assert_eq!(task.unsatisfied_dependencies, 0);
        debug_assert!(task.is_ready);
        debug_assert!(!task.scheduled);

        task.scheduled = true;

        if self.handles.is_empty() && self.waiting {
            self.waiting = false;
            circuit.log_scheduler_event(&SchedulerEvent::wait_end(circuit.global_id()));
        }

        let circuit = circuit.clone();

        if self.flush && task.flush_state == FlushState::UnflushedDependencies(0) {
            circuit.flush_node(node_id);
            task.flush_state = FlushState::Started;
        }

        self.handles.spawn_local(async move {
            let result = circuit.eval_node(node_id).await;
            (node_id, result)
        });
    }

    async fn abort(&mut self) {
        // Wait for all started tasks to abort.
        // TODO: we could use self.handles.abort_all() instead to terminate any slow (or stuck),
        // IO operations, but that requires all operators to handle cancellation gracefully.
        while !self.handles.is_empty() {
            let _ = self.handles.join_next().await;
        }
    }

    fn start_step(&mut self) {
        self.unflushed_operators = self.tasks.len();

        // Reset unsatisfied dependencies, initialize runnable queue.
        for task in self.tasks.values_mut() {
            task.flush_state = FlushState::UnflushedDependencies(task.num_predecessors);
        }
        self.flush = false
    }

    async fn microstep<C>(&mut self, circuit: &C) -> Result<(), Error>
    where
        C: Circuit,
    {
        circuit.log_scheduler_event(&SchedulerEvent::step_start(circuit.global_id()));
        let result = self.do_microstep(circuit).await;
        circuit.log_scheduler_event(&SchedulerEvent::step_end(circuit.global_id()));
        result
    }

    async fn finish_step<C>(&mut self, circuit: &C) -> Result<(), Error>
    where
        C: Circuit,
    {
        self.flush = true;
        while self.unflushed_operators > 0 {
            self.microstep(circuit).await?;
        }

        Ok(())
    }

    async fn step<C>(&mut self, circuit: &C) -> Result<(), Error>
    where
        C: Circuit,
    {
        self.start_step();
        self.finish_step(circuit).await
    }

    async fn do_microstep<C>(&mut self, circuit: &C) -> Result<(), Error>
    where
        C: Circuit,
    {
        let mut completed_tasks = 0;
        self.waiting = false;

        // Special case: empty circuit.
        if self.tasks.is_empty() {
            return Ok(());
        }

        let mut spawn = Vec::with_capacity(self.tasks.len());

        // Reset unsatisfied dependencies, initialize runnable queue.
        for task in self.tasks.values_mut() {
            task.unsatisfied_dependencies = task.num_predecessors;
            task.scheduled = false;

            if task.unsatisfied_dependencies == 0 && task.is_ready {
                spawn.push(task.node_id);
            }
        }

        for node_id in spawn.into_iter() {
            self.spawn_task(circuit, node_id);
        }

        loop {
            select! {
                ret = self.handles.join_next(), if !self.handles.is_empty() => {
                    completed_tasks += 1;

                    // The !self.handles.is_empty() check above should guarantee that this
                    // cannot be None.
                    let result = ret.expect("JoinSet::join_next returned None on a non-empty join set.");

                    let (node_id, task_result) = match result {
                        Err(error) => {
                            self.abort().await;
                            if error.is_panic() {
                                // Give our panic handler a chance to handle this.
                                panic::resume_unwind(error.into_panic());
                            } else {
                                return Err(Error::TokioError { error: error.to_string() });
                            }
                        },
                        Ok(result) => result
                    };

                    if self.tasks[&node_id].is_async {
                        self.tasks.get_mut(&node_id).unwrap().is_ready = false;
                    }

                    if let Err(error) = task_result {
                        self.abort().await;
                        return Err(error);
                    }

                    if Runtime::kill_in_progress() {
                        self.abort().await;
                        return Err(Error::Killed);
                    }

                    let flush_complete = if self.tasks[&node_id].flush_state == FlushState::Started
                        && circuit.is_flush_complete(node_id)
                    {
                        self.tasks.get_mut(&node_id).unwrap().flush_state = FlushState::Completed;
                        self.unflushed_operators -= 1;
                        true
                    } else {
                        false
                    };

                    // Are we done?
                    debug_assert!(completed_tasks <= self.tasks.len());
                    if completed_tasks == self.tasks.len() {
                        return Ok(());
                    }

                    // Spawn any new tasks that are now ready to run.
                    self.schedule_successors(circuit, node_id, flush_complete);

                    // No tasks are ready to run -- account any time until we have something to run as wait time
                    if self.handles.is_empty() {
                        self.waiting = true;
                        circuit.log_scheduler_event(
                            &SchedulerEvent::wait_start(circuit.global_id()));
                    }
                }
                _ = self.notifications.wait() => {
                    self.process_notifications(circuit);
                }
            }
        }
    }
}

pub struct DynamicScheduler(Option<RefCell<Inner>>);

impl DynamicScheduler {
    fn inner_mut(&self) -> RefMut<'_, Inner> {
        self.0
            .as_ref()
            .expect("DynamicScheduler: prepare() must be called before running the circuit")
            .borrow_mut()
    }
}

impl Scheduler for DynamicScheduler {
    fn new() -> Self {
        Self(None)
    }

    fn prepare<C>(&mut self, circuit: &C, nodes: Option<&BTreeSet<NodeId>>) -> Result<(), Error>
    where
        C: Circuit,
    {
        self.0 = Some(RefCell::new(Inner::prepare(circuit, nodes)?));
        Ok(())
    }

    async fn start_step<C>(&self, _circuit: &C) -> Result<(), Error>
    where
        C: Circuit,
    {
        let inner = &mut *self.inner_mut();

        inner.start_step();

        Ok(())
    }

    #[allow(clippy::await_holding_refcell_ref)]
    async fn microstep<C>(&self, circuit: &C) -> Result<(), Error>
    where
        C: Circuit,
    {
        let inner = &mut *self.inner_mut();

        inner.microstep(circuit).await
    }

    #[allow(clippy::await_holding_refcell_ref)]
    async fn finish_step<C>(&self, circuit: &C) -> Result<(), Error>
    where
        C: Circuit,
    {
        let inner = &mut *self.inner_mut();

        inner.finish_step(circuit).await
    }

    #[allow(clippy::await_holding_refcell_ref)]
    async fn step<C>(&self, circuit: &C) -> Result<(), Error>
    where
        C: Circuit,
    {
        let inner = &mut *self.inner_mut();

        let result = inner.step(circuit).await;
        circuit.tick();
        result
    }
}
