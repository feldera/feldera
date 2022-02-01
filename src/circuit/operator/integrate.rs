use crate::{
    algebra::{AddAssignByRef, AddByRef, HasZero},
    circuit::{
        operator::{Plus, Z1},
        Circuit, OwnershipPreference, Stream,
    },
};
use std::ops::Add;

impl<P, D> Stream<Circuit<P>, D>
where
    P: Clone + 'static,
    D: Add<Output = D> + AddByRef + AddAssignByRef + Clone + HasZero + 'static,
{
    /// Integrate the input stream.
    ///
    /// Computes the sum of values in the input stream.
    /// The first stream in the return tuple contains the value of the integral
    /// after the current clock cycle.  The second stream contains the value of the
    /// integral at the previous clock cycle, i.e., the sum of all inputs except
    /// the last one.  The latter can equivalently be obtained by applying the delay
    /// operator [`Z1`] to the integral, but this function avoids the extra storage
    /// overhead and is the preferred way to perform delayed integration.
    ///
    /// # Examples
    ///
    /// ```
    /// # use dbsp::circuit::{
    /// #     operator::Generator,
    /// #     Root,
    /// # };
    /// let root = Root::build(move |circuit| {
    ///     // Generate a stream of 1's.
    ///     let stream = circuit.add_source(Generator::new(1, |n: &mut usize| {}));
    ///     // Integrate the stream.
    ///     let (sum, delayed_sum) = stream.integrate_core();
    /// #   let mut counter1 = 0;
    /// #   sum.inspect(move |n| { counter1 += 1; assert_eq!(*n, counter1) });
    /// #   let mut counter2 = 0;
    /// #   delayed_sum.inspect(move |n| { assert_eq!(*n, counter2); counter2 += 1; });
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
    /// stream:      1, 1, 1, 1, 1, ...
    /// sum:         1, 2, 3, 4, 5, ...
    /// delayed_sum: 0, 1, 2, 3, 4, ...
    /// ```
    pub fn integrate_core(&self) -> (Stream<Circuit<P>, D>, Stream<Circuit<P>, D>) {
        // Integration circuit:
        //
        // ```
        //           ┌───┐
        //    ──────►│   ├─────►
        //           │ + │
        //      ┌───►│   ├────┐
        //      │    └───┘    │
        //      │             │
        //      │    ┌───┐    │
        //      │    │   │    │
        //      └────┤z-1├────┘
        //           │   │
        //           └───┘
        // ```
        let (z, feedback) = self.circuit().add_feedback(Z1::new(D::zero()));
        let adder = self.circuit().add_binary_operator_with_preference(
            Plus::new(),
            &z,
            self,
            OwnershipPreference::STRONGLY_PREFER_OWNED,
            OwnershipPreference::PREFER_OWNED,
        );
        feedback.connect_with_preference(&adder, OwnershipPreference::STRONGLY_PREFER_OWNED);
        (adder, z)
    }

    /// Integrate the input stream.
    ///
    /// The output stream contains the sum of all values in the input stream.
    ///
    /// # Examples
    ///
    /// ```text
    /// input stream: 1, 1, 1, 1, ...
    /// outpus stream 0, 1, 2, 3, ...
    /// ```
    pub fn integrate(&self) -> Stream<Circuit<P>, D> {
        self.integrate_core().0
    }

    /// Delayed integration.
    ///
    /// The output stream contains the sum of all values in the input stream
    /// excluding the last clock cycle.
    ///
    /// # Examples
    ///
    /// ```text
    /// input stream: 1, 1, 1, 1, ...
    /// outpus stream 0, 1, 2, 3, ...
    /// ```
    pub fn integrate_delayed(&self) -> Stream<Circuit<P>, D> {
        self.integrate_core().1
    }
}

#[cfg(test)]
mod test {
    use crate::{
        algebra::{finite_map::FiniteMap, zset::ZSetHashMap},
        circuit::{operator::Generator, Root},
    };

    #[test]
    fn scalar_integrate() {
        let root = Root::build(move |circuit| {
            let source = circuit.add_source(Generator::new(1, |_: &mut usize| {}));
            let mut counter = 0;
            source.integrate().inspect(move |n| {
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
            let source = circuit.add_source(Generator::new(
                ZSetHashMap::new(),
                move |s: &mut ZSetHashMap<usize, isize>| {
                    s.increment(&counter1, 1);
                    counter1 += 1;
                },
            ));

            let (integral, integral_delayed) = source.integrate_core();
            let mut counter2 = 0;
            integral.inspect(move |s| {
                for i in 0..counter2 {
                    assert_eq!(s.lookup(&i), (counter2 - i) as isize);
                }
                counter2 += 1;
                assert_eq!(s.lookup(&counter2), 0);
            });
            let mut counter3 = 0;
            integral_delayed.inspect(move |s| {
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
