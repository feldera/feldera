//! Create subcircuits that iterate until a specified condition
//! defined over the contents of a stream is satisfied.

use crate::circuit::{
    schedule::{Error as SchedulerError, Scheduler},
    Circuit, Stream,
};
use std::{cell::RefCell, marker::PhantomData, rc::Rc};

impl<P, D> Stream<Circuit<P>, D>
where
    P: 'static + Clone,
    D: 'static + Clone,
{
    /// Attach a condition to a stream.
    ///
    /// A [`Condition`] is a condition on the value in the stream
    /// checked on each clock cycle, that can be used to terminate
    /// the execution of the subcircuit (see [`Circuit::iterate_with_condition`]
    /// and [`Circuit::iterate_with_conditions`]).
    pub fn condition<F>(&self, condition_func: F) -> Condition<Circuit<P>>
    where
        F: 'static + Fn(&D) -> bool,
    {
        let cond = Rc::new(RefCell::new(false));
        let cond_clone = cond.clone();
        self.inspect(move |v| *cond_clone.borrow_mut() = condition_func(v));
        Condition::new(cond)
    }
}

impl<P> Circuit<P>
where
    P: 'static + Clone,
{
    /// Create a subcircuit that iterates until a condition is satisfied.
    ///
    /// This method is similar to [`Circuit::iterate`], which creates
    /// a subcircuit that iterates until a specified condition is
    /// satisfied, but here the condition is a predicate over the
    /// contents of a stream captured by a [`Condition`].
    ///
    /// The `constructor` closure populates the child circuit and returns
    /// a condition that will be evaluated to check the termination
    /// condition on each iteration and an arbitrary user-defined return
    /// value that typically contains output streams of the child.
    /// The subcircuit will iterate until the condition returns true.
    pub fn iterate_with_condition<F, T>(&self, constructor: F) -> Result<T, SchedulerError>
    where
        F: FnOnce(&mut Circuit<Self>) -> Result<(Condition<Circuit<Self>>, T), SchedulerError>,
    {
        self.iterate(|child| {
            let (condition, res) = constructor(child)?;
            Ok((move || condition.check(), res))
        })
    }

    /// Similar to `Self::iterate_with_condition`, but with a user-specified
    /// [`Scheduler`].
    pub fn iterate_with_condition_and_scheduler<F, T, S>(
        &self,
        constructor: F,
    ) -> Result<T, SchedulerError>
    where
        F: FnOnce(&mut Circuit<Self>) -> Result<(Condition<Circuit<Self>>, T), SchedulerError>,
        S: Scheduler + 'static,
    {
        self.iterate_with_scheduler::<_, _, _, S>(|child| {
            let (condition, res) = constructor(child)?;
            Ok((move || condition.check(), res))
        })
    }

    /// Create a subcircuit that iterates until multiple conditions are
    /// satisfied.
    ///
    /// Similar to `Self::iterate_with_condition`, but allows the subcircuit to
    /// have multiple conditions.  The subcircuit will iterate until _all_
    /// conditions are satisfied _simultaneously_ in the same clock cycle.
    pub fn iterate_with_conditions<F, T>(&self, constructor: F) -> Result<T, SchedulerError>
    where
        F: FnOnce(&mut Circuit<Self>) -> Result<(Vec<Condition<Circuit<Self>>>, T), SchedulerError>,
    {
        self.iterate(|child| {
            let (conditions, res) = constructor(child)?;
            Ok((move || conditions.iter().all(Condition::check), res))
        })
    }

    /// Similar to `Self::iterate_with_conditions`, but with a user-specified
    /// [`Scheduler`].
    pub fn iterate_with_conditions_and_scheduler<F, T, S>(
        &self,
        constructor: F,
    ) -> Result<T, SchedulerError>
    where
        F: FnOnce(&mut Circuit<Self>) -> Result<(Vec<Condition<Circuit<Self>>>, T), SchedulerError>,
        S: 'static + Scheduler,
    {
        self.iterate_with_scheduler::<_, _, _, S>(|child| {
            let (conditions, res) = constructor(child)?;
            Ok((move || conditions.iter().all(Condition::check), res))
        })
    }
}

/// A condition attached to a stream that can be used
/// to terminate the execution of a subcircuit
/// (see [`Circuit::iterate_with_condition`] and
/// [`Circuit::iterate_with_conditions`]).
///
/// A condition is created by the [`Stream::condition`] method.
pub struct Condition<C> {
    cond: Rc<RefCell<bool>>,
    _phantom: PhantomData<C>,
}

impl<C> Condition<C> {
    fn new(cond: Rc<RefCell<bool>>) -> Self {
        Self {
            cond,
            _phantom: PhantomData,
        }
    }

    fn check(&self) -> bool {
        *self.cond.borrow()
    }
}

#[cfg(test)]
mod test {
    use crate::{
        algebra::{FiniteMap, ZSet, ZSetHashMap},
        circuit::{
            schedule::{DynamicScheduler, Scheduler, StaticScheduler},
            trace::TraceMonitor,
            Circuit, Root, Stream,
        },
        finite_map,
        operator::{Apply2, Generator, Z1},
    };
    use std::sync::{Arc, Mutex};

    #[test]
    fn iterate_with_conditions_static() {
        iterate_with_conditions::<StaticScheduler>();
    }

    #[test]
    fn iterate_with_conditions_dynamic() {
        iterate_with_conditions::<DynamicScheduler>();
    }

    fn iterate_with_conditions<S>()
    where
        S: Scheduler + 'static,
    {
        let root = Root::build_with_scheduler::<_, S>(|circuit| {
            TraceMonitor::attach(
                Arc::new(Mutex::new(TraceMonitor::new_panic_on_error())),
                circuit,
                "monitor",
            );
            // Graph edges
            let edges = circuit.add_source(Generator::new(move || finite_map! {
                (0, 3) => 1,
                (1, 2) => 1,
                (2, 1) => 1,
                (3, 1) => 1,
                (3, 4) => 1,
                (4, 5) => 1,
                (4, 6) => 1,
                (5, 6) => 1,
                (5, 1) => 1,
            }));

            // Two sets of initial states.  The inner circuit computes sets of nodes reachable
            // from each of these initial sets.
            let init1 = circuit.add_source(Generator::new(|| finite_map! { 1 => 1, 2 => 1, 3 => 1 }));
            let init2 = circuit.add_source(Generator::new(|| finite_map! { 4 => 1 }));

            let (reachable1, reachable2) = circuit.iterate_with_conditions_and_scheduler::<_, _, S>(|child| {
                let edges = edges.delta0(child).integrate().current;
                let init1 = init1.delta0(child).integrate().current;
                let init2 = init2.delta0(child).integrate().current;

                // Builds a subcircuit that computes nodes reachable from `init`:
                //
                // ```
                //  init
                // ────────────► + ─────┐
                //               ▲      │
                //               │      │
                //         ┌─────┘    distinct
                //         │            │
                //        suc ◄─── z ◄──┘
                //                 │
                //                 └───────────►
                // ```
                //
                // where suc computes the set of successor nodes.
                let reachable_circuit = |init: Stream<Circuit<Circuit<()>>, ZSetHashMap<usize, isize>>| {
                    let (prev_reachable, feedback) = child.add_feedback_with_export(Z1::new(ZSetHashMap::new()));

                    // Computes all successors of `nodes`.
                    let successor_set = |nodes: &ZSetHashMap<usize, isize>, edges: &ZSetHashMap<(usize, usize), isize>| {
                        nodes.join(edges, |node| *node, |(from, _to)| *from, |_node, (_from, to)| *to)
                    };
                    let suc = child
                        .add_binary_operator(
                            Apply2::new(successor_set.clone()),
                            &prev_reachable.local,
                            &edges,
                        );

                    let reachable = init.plus(&suc).apply(ZSet::distinct);
                    feedback.connect(&reachable);
                    let condition = reachable.differentiate().condition(|z| z.support_size() == 0);
                    (condition, prev_reachable.export)
                };

                let (condition1, export1) = reachable_circuit(init1);
                let (condition2, export2) = reachable_circuit(init2);

                Ok((vec![condition1, condition2], (export1, export2)))
            })
            .unwrap();

            reachable1.inspect(|r| assert_eq!(r, &finite_map! { 1 => 1, 2 => 1, 3 => 1, 4 => 1, 5 => 1, 6 => 1}));
            reachable2.inspect(|r| assert_eq!(r, &finite_map! { 1 => 1, 2 => 1, 4 => 1, 5 => 1, 6 => 1}));
        })
        .unwrap();

        for _ in 0..3 {
            root.step().unwrap();
        }
    }
}
