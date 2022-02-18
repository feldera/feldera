use crate::{
    algebra::{AddAssignByRef, AddByRef, HasZero},
    circuit::{Circuit, OwnershipPreference, Stream},
    operator::{Plus, Z1},
};
use std::ops::Add;

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
}

#[cfg(test)]
mod test {
    use crate::{
        algebra::{FiniteMap, MapBuilder, ZSetHashMap},
        circuit::Root,
        operator::Generator,
    };

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
}
