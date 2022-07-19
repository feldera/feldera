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
            Ok((move || Ok(condition.check()), res))
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
            Ok((move || Ok(condition.check()), res))
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
            Ok((move || Ok(conditions.iter().all(Condition::check)), res))
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
            Ok((move || Ok(conditions.iter().all(Condition::check)), res))
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
        circuit::{
            schedule::{DynamicScheduler, Scheduler, StaticScheduler},
            Circuit, Root, Stream,
        },
        monitor::TraceMonitor,
        operator::{DelayedFeedback, FilterMap, Generator},
        trace::{
            ord::{OrdIndexedZSet, OrdZSet},
            BatchReader,
        },
        zset,
    };

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
            TraceMonitor::new_panic_on_error().attach(circuit, "monitor");

            // Graph edges
            let edges = circuit.add_source(Generator::new(move || {
                zset! {
                    (0, 3) => 1,
                    (1, 2) => 1,
                    (2, 1) => 1,
                    (3, 1) => 1,
                    (3, 4) => 1,
                    (4, 5) => 1,
                    (4, 6) => 1,
                    (5, 6) => 1,
                    (5, 1) => 1,
                }
            }));

            // Two sets of initial states.  The inner circuit computes sets of nodes
            // reachable from each of these initial sets.
            let init1 = circuit.add_source(Generator::new(|| zset! { 1 => 1, 2 => 1, 3 => 1 }));
            let init2 = circuit.add_source(Generator::new(|| zset! { 4 => 1 }));

            let (reachable1, reachable2) = circuit
                .iterate_with_conditions_and_scheduler::<_, _, S>(|child| {
                    let edges = edges.delta0(child).integrate();
                    let init1 = init1.delta0(child).integrate();
                    let init2 = init2.delta0(child).integrate();

                    let edges_indexed: Stream<_, OrdIndexedZSet<usize, usize, isize>> =
                        edges.index();

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
                    let reachable_circuit =
                        |init: Stream<Circuit<Circuit<()>>, OrdZSet<usize, isize>>| {
                            let feedback = <DelayedFeedback<_, OrdZSet<usize, isize>>>::new(child);

                            let feedback_pairs: Stream<_, OrdZSet<(usize, ()), isize>> =
                                feedback.stream().map(|&node| (node, ()));
                            let feedback_indexed: Stream<_, OrdIndexedZSet<usize, (), isize>> =
                                feedback_pairs.index();

                            let suc =
                                feedback_indexed.stream_join(&edges_indexed, |_node, &(), &to| to);

                            let reachable = init.plus(&suc).distinct();
                            feedback.connect(&reachable);
                            let condition = reachable.differentiate().condition(|z| z.len() == 0);
                            (condition, reachable.export())
                        };

                    let (condition1, export1) = reachable_circuit(init1);
                    let (condition2, export2) = reachable_circuit(init2);

                    Ok((vec![condition1, condition2], (export1, export2)))
                })
                .unwrap();

            reachable1.inspect(|r| {
                assert_eq!(r, &zset! { 1 => 1, 2 => 1, 3 => 1, 4 => 1, 5 => 1, 6 => 1})
            });
            reachable2.inspect(|r| assert_eq!(r, &zset! { 1 => 1, 2 => 1, 4 => 1, 5 => 1, 6 => 1}));
        })
        .unwrap();

        for _ in 0..3 {
            root.step().unwrap();
        }
    }
}
