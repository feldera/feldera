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
    collections::{HashMap, HashSet},
    ops::Deref,
    sync::{Arc, Mutex},
    thread::Thread,
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
use priority_queue::PriorityQueue;

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

    /// Scheduling priority.  The scheduler picks the top priority node out
    /// of all runnable nodes in the current state.
    priority: isize,

    /// `true` if this is an async node.  The node can only be evaluated in a
    /// ready state.
    is_async: bool,

    // Mutable fields.
    /// Number of predecessors not yet evaluated.  Set to `num_predecessors`
    /// at the start of each step.
    unsatisfied_dependencies: usize,

    /// `true` if the async node is known to be in a ready state.  Always
    /// `true` for non-async nodes.
    is_ready: bool,

    /// Task has been scheduled (put on the run queue) in the current clock
    /// cycle.
    scheduled: bool,
}

/// The set of async nodes for which the scheduler has received ready
/// notifications.
#[derive(Clone)]
struct Notifications {
    /// Nodes that received notifications.
    nodes: Arc<Mutex<HashSet<NodeId>>>,

    /// Handle to wake up the scheduler thread when a notification arrives.
    unparker: Thread,
}

impl Notifications {
    fn new(size: usize, unparker: Thread) -> Self {
        Self {
            nodes: Arc::new(Mutex::new(HashSet::with_capacity(size))),
            unparker,
        }
    }

    /// Add a new notification.
    fn notify(&self, node_id: NodeId) {
        self.nodes.lock().unwrap().insert(node_id);
        self.unparker.unpark();
    }
}

/// Runnable tasks sorted by priority.
struct RunQueue(PriorityQueue<NodeId, isize>);

impl RunQueue {
    fn with_capacity(capacity: usize) -> Self {
        Self(PriorityQueue::with_capacity(capacity))
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Add `task` to runnable queue.
    fn push(&mut self, task: &mut Task) {
        debug_assert!(task.unsatisfied_dependencies == 0);
        debug_assert!(task.is_ready);
        debug_assert!(!task.scheduled);

        self.0.push(task.node_id, task.priority);
        task.scheduled = true;
    }

    fn pop(&mut self) -> Option<(NodeId, isize)> {
        self.0.pop()
    }
}

/// Dynamic scheduler internals.
struct Inner {
    // Immutable fields (initialized once when preparing the scheduler).
    /// List of tasks that must be evaluated at each clock cycle.
    /// Tasks are stored in the same order as nodes in the circuit and
    /// task index is equal to the node id.
    tasks: Vec<Task>,

    // Mutable fields.
    /// Ready notifications received while the scheduler was busy or sleeping.
    notifications: Notifications,

    /// Tasks that are ready to be executed.
    runnable: RunQueue,
}

impl Inner {
    /// Dequeue a highest-priority task from the runnable queue.
    /// Update all successors of the task, reducing their unsatisfied
    /// dependencies by 1.  Move successors to the runnable queue
    /// when possible.
    fn dequeue_next_task(&mut self) -> Option<NodeId> {
        if let Some((node_id, _)) = self.runnable.pop() {
            let id = node_id.id();
            debug_assert!(id < self.tasks.len());

            // Update its successor dependencies.

            // Don't use iterator, as we will borrow `tasks` again below.
            for i in 0..self.tasks[id].successors.len() {
                let succ_id = self.tasks[id].successors[i];
                debug_assert!(succ_id.id() < self.tasks.len());
                let successor = &mut self.tasks[succ_id.id()];
                debug_assert!(successor.unsatisfied_dependencies != 0);
                successor.unsatisfied_dependencies -= 1;
                if successor.unsatisfied_dependencies == 0 && successor.is_ready {
                    self.runnable.push(successor);
                }
            }
            Some(node_id)
        } else {
            None
        }
    }

    /// Process and dequeue new notifications.
    fn process_notifications<C>(&mut self, circuit: &C)
    where
        C: Circuit,
    {
        let mut nodes = self.notifications.nodes.lock().unwrap();

        // False positive via rust-clippy/#8963
        #[allow(unknown_lints)]
        #[allow(clippy::significant_drop_in_scrutinee)]
        for id in nodes.drain() {
            let task = &mut self.tasks[id.id()];
            debug_assert!(task.is_async);

            // Ignore duplicate notifications.
            if task.is_ready {
                continue;
            }

            // Ignore spurious notifications.
            if circuit.ready(id) {
                task.is_ready = true;

                // We can see a notification for an already scheduled task
                // indicating that it's become ready again.
                // This notification should take effect at the next clock
                // cycle.
                if task.unsatisfied_dependencies == 0 && !task.scheduled {
                    self.runnable.push(task);
                }
            }
        }
    }

    fn prepare<C>(circuit: &C) -> Result<Self, Error>
    where
        C: Circuit,
    {
        // Check that ownership constraints don't introduce cycles.
        let mut g = circuit_graph(circuit);

        let extra_constraints = ownership_constraints(circuit)?;

        for (from, to) in extra_constraints.iter() {
            g.add_edge(*from, *to, ());
        }

        // `toposort` fails if the graph contains cycles.
        toposort(&g, None).map_err(|e| Error::CyclicCircuit {
            node_id: GlobalNodeId::child_of(circuit, e.node_id()),
        })?;

        let num_nodes = circuit.num_nodes();
        let mut successors: HashMap<NodeId, Vec<NodeId>> = HashMap::with_capacity(num_nodes);
        let mut predecessors: HashMap<NodeId, Vec<NodeId>> = HashMap::with_capacity(num_nodes);

        for edge in circuit.edges().iter() {
            successors.entry(edge.from).or_default().push(edge.to);

            predecessors.entry(edge.to).or_default().push(edge.from);
        }

        // Add ownership constraints to the graph.
        for (from, to) in extra_constraints.into_iter() {
            successors.entry(from).or_default().push(to);
            predecessors.entry(to).or_default().push(from);
        }

        let mut tasks = Vec::with_capacity(num_nodes);
        let mut num_async_nodes = 0;

        for (i, node_id) in circuit.node_ids().into_iter().enumerate() {
            // We rely on node id to be equal to its index.
            assert!(i == node_id.id());

            // A naive heuristic priority assignment algorithm
            // (not sure if it is any good).
            // We attempt to minimize the amount of data buffered in
            // streams during the evaluation of the circuit.
            let num_predecessors = predecessors.entry(node_id).or_default().len();
            let num_successors = successors.entry(node_id).or_default().len();
            let priority = num_predecessors as isize - num_successors as isize;

            let is_async = circuit.is_async_node(node_id);
            if is_async {
                num_async_nodes += 1;
            }

            tasks.push(Task {
                node_id,
                num_predecessors,
                successors: successors.entry(node_id).or_default().clone(),
                priority,
                is_async,
                unsatisfied_dependencies: num_predecessors,
                is_ready: !is_async,
                scheduled: false,
            });
        }

        let scheduler = Self {
            tasks,
            notifications: Notifications::new(num_async_nodes, std::thread::current()),
            runnable: RunQueue::with_capacity(num_nodes),
        };

        // Setup scheduler callbacks.
        for node_id in circuit.node_ids().into_iter() {
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

    fn step<C>(&mut self, circuit: &C) -> Result<(), Error>
    where
        C: Circuit,
    {
        circuit.log_scheduler_event(&SchedulerEvent::step_start(circuit.global_id().deref()));

        let mut completed_tasks = 0;

        // Reset unsatisfied dependencies, initialize runnable queue.
        for task in self.tasks.iter_mut() {
            task.unsatisfied_dependencies = task.num_predecessors;
            task.scheduled = false;

            if task.unsatisfied_dependencies == 0 && task.is_ready {
                self.runnable.push(task);
            }
        }

        while completed_tasks < self.tasks.len() {
            if Runtime::kill_in_progress() {
                return Err(Error::Killed);
            }

            match self.dequeue_next_task() {
                None => {
                    // No more tasks in the run queue -- try to add some by
                    // processing notifications.
                    self.process_notifications(circuit);

                    // Still nothing to do -- sleep waiting for a notification to
                    // unpark us.
                    if self.runnable.is_empty() {
                        circuit.log_scheduler_event(&SchedulerEvent::wait_start(
                            circuit.global_id().deref(),
                        ));
                        std::thread::park();
                        circuit.log_scheduler_event(&SchedulerEvent::wait_end(
                            circuit.global_id().deref(),
                        ));
                    }
                }

                Some(node_id) => {
                    circuit.eval_node(node_id)?;
                    if self.tasks[node_id.id()].is_async {
                        self.tasks[node_id.id()].is_ready = false;
                    }

                    completed_tasks += 1;
                }
            }
        }
        circuit.tick();

        circuit.log_scheduler_event(&SchedulerEvent::step_end(circuit.global_id().deref()));
        Ok(())
    }
}

pub struct DynamicScheduler(RefCell<Inner>);

impl DynamicScheduler {
    fn inner_mut(&self) -> RefMut<'_, Inner> {
        self.0.borrow_mut()
    }
}

impl Scheduler for DynamicScheduler {
    fn prepare<C>(circuit: &C) -> Result<Self, Error>
    where
        C: Circuit,
    {
        Ok(Self(RefCell::new(Inner::prepare(circuit)?)))
    }

    fn step<C>(&self, circuit: &C) -> Result<(), Error>
    where
        C: Circuit,
    {
        self.inner_mut().step(circuit)
    }
}
