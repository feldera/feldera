use crate::{
    algebra::{AddAssignByRef, AddByRef, HasZero},
    circuit::{Circuit, OwnershipPreference, Stream},
    operator::{BinaryOperatorAdapter, Plus, Z1Nested, Z1},
};
use std::{ops::Add, rc::Rc};

/// Struct returned by the [`Stream::integrate`] operation.
///
/// This struct bundles the four output streams produced by the `integrate`
/// method:
///
/// * `current` - the current value of the integral, i.e., the sum of all values
///   in the stream since the last `clock_start` including the current clock
///   cycle.
/// * `delayed` - the previous value of the integral, i.e., the sum of all
///   values in the stream since the last `clock_start` up to the previous clock
///   cycle.
/// * `export` - stream exported to the parent circuit that contains the final
///   value of the integral at the end of the nested clock epoch.
/// * `input` - value added to the integral at the last clock cycle.  This is
///   just a reference to the input stream (the stream being integrated).  We
///   bundle it here as it is often used together with other outputs, e.g., by
///   the incremental join operator.
pub struct StreamIntegral<P, I> {
    pub current: Stream<Circuit<P>, I>,
    pub delayed: Stream<Circuit<P>, I>,
    pub export: Stream<P, I>,
    pub input: Stream<Circuit<P>, I>,
}

impl<P, I> StreamIntegral<P, I> {
    fn new(
        current: Stream<Circuit<P>, I>,
        delayed: Stream<Circuit<P>, I>,
        export: Stream<P, I>,
        input: Stream<Circuit<P>, I>,
    ) -> Self {
        Self {
            current,
            delayed,
            export,
            input,
        }
    }
}

/// Struct returned by the [`Stream::integrate_nested`] operation.
///
/// This struct bundles the four output streams produced by the
/// `integrate_nested` method. Below, we denote `stream[i,j]` the value of
/// `stream` at time `[i,j]`, where `i` is the parent timestamp, and `j` is the
/// child timestamp.
///
/// * `current` - the current value of the integral: `current[i,j] =
///   sum(input[k,j]), k<=i`.
/// * `delayed` - the previous value of the integral: `delayed[i,j] =
///   sum(input[k,j]), k<i`.
/// * `export` - stream exported to the parent circuit that contains the final
///   value of the integral at the end of the nested clock epoch.
/// * `input` - reference to the input stream.
pub struct NestedStreamIntegral<P, I> {
    pub current: Stream<Circuit<P>, Rc<I>>,
    pub delayed: Stream<Circuit<P>, Rc<I>>,
    pub export: Stream<P, Rc<I>>,
    pub input: Stream<Circuit<P>, I>,
}

impl<P, I> NestedStreamIntegral<P, I> {
    fn new(
        current: Stream<Circuit<P>, Rc<I>>,
        delayed: Stream<Circuit<P>, Rc<I>>,
        export: Stream<P, Rc<I>>,
        input: Stream<Circuit<P>, I>,
    ) -> Self {
        Self {
            current,
            delayed,
            export,
            input,
        }
    }
}

impl<P, D> Stream<Circuit<P>, D>
where
    P: Clone + 'static,
    D: Add<Output = D> + AddByRef + AddAssignByRef + Clone + HasZero + 'static,
{
    /// Integrate the input stream.
    ///
    /// Computes the sum of values in the input stream.
    /// The first stream in the return tuple contains the value of the integral
    /// after the current clock cycle.  The second stream contains the value of
    /// the integral at the previous clock cycle, i.e., the sum of all
    /// inputs except the last one.  The latter can equivalently be obtained
    /// by applying the delay operator [`Z1`] to the integral, but this
    /// function avoids the extra storage overhead and is the preferred way
    /// to perform delayed integration.
    ///
    /// # Examples
    ///
    /// ```
    /// # use dbsp::{
    /// #     circuit::Root,
    /// #     operator::Generator,
    /// # };
    /// let root = Root::build(move |circuit| {
    ///     // Generate a stream of 1's.
    ///     let stream = circuit.add_source(Generator::new(|| 1));
    ///     // Integrate the stream.
    ///     let integral = stream.integrate();
    /// #   let mut counter1 = 0;
    /// #   integral.current.inspect(move |n| { counter1 += 1; assert_eq!(*n, counter1) });
    /// #   let mut counter2 = 0;
    /// #   integral.delayed.inspect(move |n| { assert_eq!(*n, counter2); counter2 += 1; });
    /// })
    /// .unwrap();
    ///
    /// # for _ in 0..5 {
    /// #     root.step().unwrap();
    /// # }
    /// ```
    ///
    /// Streams in the above example will contain the following values:
    ///
    /// ```text
    /// input:   1, 1, 1, 1, 1, ...
    /// current: 1, 2, 3, 4, 5, ...
    /// delayed: 0, 1, 2, 3, 4, ...
    /// ```
    pub fn integrate(&self) -> StreamIntegral<P, D> {
        // Integration circuit:
        // ```
        //              input
        //   ┌─────────────────►
        //   │
        //   │    ┌───┐ current
        // ──┴───►│   ├────────►
        //        │ + │
        //   ┌───►│   ├────┐
        //   │    └───┘    │
        //   │             │
        //   │    ┌───┐    │
        //   │    │   │    │
        //   └────┤z-1├────┘
        //        │   │
        //        └───┴────────►
        //              delayed
        //              export
        // ```
        let (z, feedback) = self.circuit().add_feedback_with_export(Z1::new(D::zero()));
        let adder = self.circuit().add_binary_operator_with_preference(
            Plus::new(),
            &z.local,
            self,
            OwnershipPreference::STRONGLY_PREFER_OWNED,
            OwnershipPreference::PREFER_OWNED,
        );
        feedback.connect_with_preference(&adder, OwnershipPreference::STRONGLY_PREFER_OWNED);
        StreamIntegral::new(adder, z.local, z.export, self.clone())
    }

    /// Integrate stream of streams.
    ///
    /// Computes the sum of nested streams, i.e., rather than integrating values
    /// in each nested stream, this function sums up entire input streams
    /// across all parent timestamps, where the sum of streams is defined as
    /// a stream of point-wise sums of their elements: `integral[i,j] =
    /// sum(input[k,j]), k<=i`, where `stream[i,j]` is the value of `stream`
    /// at time `[i,j]`, `i` is the parent timestamp, and `j` is the child
    /// timestamp.
    ///
    /// Yields the sum element-by-element as the input stream is fed to the
    /// integral.
    ///
    /// # Examples
    ///
    /// Input stream (one row per parent timestamps):
    ///
    /// ```text
    /// 1 2 3 4
    /// 1 1 1 1 1
    /// 2 2 2 0 0
    /// ```
    ///
    /// Integral:
    ///
    /// ```text
    /// 1 2 3 4
    /// 2 3 4 5 1
    /// 4 5 6 5 1
    /// ```
    pub fn integrate_nested(&self) -> NestedStreamIntegral<P, D> {
        let (z, feedback) = self
            .circuit()
            .add_feedback_with_export(Z1Nested::new(Rc::new(D::zero())));
        let adder = self.circuit().add_binary_operator_with_preference(
            <BinaryOperatorAdapter<D, D, D, _>>::new(Plus::new()),
            &z.local,
            self,
            OwnershipPreference::STRONGLY_PREFER_OWNED,
            OwnershipPreference::PREFER_OWNED,
        );
        feedback.connect_with_preference(&adder, OwnershipPreference::STRONGLY_PREFER_OWNED);
        NestedStreamIntegral::new(adder, z.local, z.export, self.clone())
    }
}

#[cfg(test)]
mod test {
    use crate::{
        algebra::{FiniteMap, MapBuilder, ZSetHashMap},
        circuit::{trace::TraceMonitor, Root},
        operator::{Generator, Z1},
    };

    use std::sync::{Arc, Mutex};

    #[test]
    fn scalar_integrate() {
        let root = Root::build(move |circuit| {
            let source = circuit.add_source(Generator::new(|| 1));
            let mut counter = 0;
            source.integrate().current.inspect(move |n| {
                counter += 1;
                assert_eq!(*n, counter);
            });
        })
        .unwrap();

        for _ in 0..100 {
            root.step().unwrap();
        }
    }

    #[test]
    fn zset_integrate() {
        let root = Root::build(move |circuit| {
            let mut counter1 = 0;
            let mut s = ZSetHashMap::new();
            let source = circuit.add_source(Generator::new(move || {
                let res = s.clone();
                s.increment(&counter1, 1);
                counter1 += 1;
                res
            }));

            let integral = source.integrate();
            let mut counter2 = 0;
            integral.current.inspect(move |s| {
                for i in 0..counter2 {
                    assert_eq!(s.lookup(&i), (counter2 - i) as isize);
                }
                counter2 += 1;
                assert_eq!(s.lookup(&counter2), 0);
            });
            let mut counter3 = 0;
            integral.delayed.inspect(move |s| {
                for i in 1..counter3 {
                    assert_eq!(s.lookup(&(i - 1)), (counter3 - i) as isize);
                }
                counter3 += 1;
                assert_eq!(s.lookup(&(counter3 - 1)), 0);
            });
        })
        .unwrap();

        for _ in 0..100 {
            root.step().unwrap();
        }
    }

    /// ```text
    ///            ┌───────────────────────────────────────────────────────────────────────────────────┐
    ///            │                                                                                   │
    ///            │                           3,2,1,0,0,0                     3,2,1,0,                │
    ///            │                           4,3,2,1,0,0                     7,5,3,1,0,              │
    ///            │                    ┌───┐  2,1,0,0,0,0                     9,6,3,1,0,              │
    ///  3,4,2,5   │                    │   │  5,4,3,2,1,0                     14,10,6,3,1,0           │ 6,16,19,34
    /// ───────────┼──►delta0──────────►│ + ├──────────┬─────►integrate_nested───────────────integrate─┼─────────────►
    ///            │          3,0,0,0,0 │   │          │                                               │
    ///            │          4,0,0,0,0 └───┘          ▼                                               │
    ///            │          2,0,0,0,0   ▲    ┌──┐   ┌───┐                                            │
    ///            │          5,0,0,0,0   └────┤-1│◄──|z-1|                                            │
    ///            │                           └──┘   └───┘                                            │
    ///            │                                                                                   │
    ///            └───────────────────────────────────────────────────────────────────────────────────┘
    /// ```
    #[test]
    fn scalar_integrate_nested() {
        let root = Root::build(move |circuit| {
            TraceMonitor::attach(
                Arc::new(Mutex::new(TraceMonitor::new_panic_on_error())),
                circuit,
                "monitor",
            );

            let mut input = vec![3, 4, 2, 5].into_iter();

            let mut expected_counters =
                vec![3, 2, 1, 0, 4, 3, 2, 1, 0, 2, 1, 0, 0, 0, 5, 4, 3, 2, 1, 0].into_iter();
            let mut expected_integrals =
                vec![3, 2, 1, 0, 7, 5, 3, 1, 0, 9, 6, 3, 1, 0, 14, 10, 6, 3, 1, 0].into_iter();
            let mut expected_outer_integrals = vec![6, 16, 19, 34].into_iter();

            let source = circuit.add_source(Generator::new(move || input.next().unwrap()));
            let integral = circuit
                .iterate_with_condition(|child| {
                    let source = source.delta0(&child);
                    let (z, feedback) = child.add_feedback(Z1::new(0));
                    let plus = source.plus(&z.apply(|n| if *n > 0 { *n - 1 } else { *n }));
                    plus.inspect(move |n| assert_eq!(*n, expected_counters.next().unwrap()));
                    feedback.connect(&plus);
                    let integral = plus.integrate_nested();
                    integral
                        .current
                        .inspect(move |n| assert_eq!(**n, expected_integrals.next().unwrap()));
                    Ok((
                        integral.current.condition(|n| **n == 0),
                        integral.current.apply(|rc| **rc).integrate().export,
                    ))
                })
                .unwrap();
            integral.inspect(move |n| assert_eq!(*n, expected_outer_integrals.next().unwrap()))
        })
        .unwrap();

        for _ in 0..4 {
            root.step().unwrap();
        }
    }
}
