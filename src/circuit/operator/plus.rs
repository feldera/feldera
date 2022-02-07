//! Binary plus operator.

use crate::{
    algebra::{AddAssignByRef, AddByRef},
    circuit::{
        operator_traits::{BinaryOperator, Operator, UnaryOperator},
        Circuit, OwnershipPreference, Stream,
    },
};
use std::{borrow::Cow, marker::PhantomData, ops::Add};

impl<P, D> Stream<Circuit<P>, D>
where
    P: Clone + 'static,
    D: Add<Output = D> + AddByRef + AddAssignByRef + Clone + 'static,
{
    /// Apply the [`Plus`] operator to `self` and `other`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use dbsp::circuit::{
    /// #     operator::Generator,
    /// #     Root,
    /// # };
    /// let root = Root::build(move |circuit| {
    ///     // Stream of non-negative values: 0, 1, 2, ...
    ///     let mut n = 0;
    ///     let source1 = circuit.add_source(Generator::new(move || {let res = n; n += 1; res}));
    ///     // Stream of non-positive values: 0, -1, -2, ...
    ///     let mut n = 0;
    ///     let source2 = circuit.add_source(Generator::new(move || {let res = n; n -= 1; res}));
    ///     // Compute pairwise sums of values in the stream; the output stream will contain zeros.
    ///     source1.plus(&source2).inspect(|n| assert_eq!(*n, 0));
    /// })
    /// .unwrap();
    ///
    /// # for _ in 0..5 {
    /// #     root.step().unwrap();
    /// # }
    /// ```
    pub fn plus(&self, other: &Stream<Circuit<P>, D>) -> Stream<Circuit<P>, D> {
        if self.origin_node_id() == other.origin_node_id() {
            self.circuit().add_unary_operator(Times2::new(), self)
        } else {
            self.circuit()
                .add_binary_operator(Plus::new(), self, other)
                .unwrap()
        }
    }
}

/// Operator that computes the sum of values in its two input streams at each timestamp.
pub struct Plus<D> {
    phantom: PhantomData<D>,
}

impl<D> Plus<D> {
    pub const fn new() -> Self {
        Self {
            phantom: PhantomData,
        }
    }
}

impl<D> Operator for Plus<D>
where
    D: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Plus")
    }

    fn clock_start(&mut self) {}
    fn clock_end(&mut self) {}
}

impl<D> BinaryOperator<D, D, D> for Plus<D>
where
    D: Add<Output = D> + AddByRef + AddAssignByRef + Clone + 'static,
{
    fn eval(&mut self, i1: &D, i2: &D) -> D {
        i1.add_by_ref(i2)
    }

    fn eval_owned_and_ref(&mut self, mut i1: D, i2: &D) -> D {
        i1.add_assign_by_ref(i2);
        i1
    }

    fn eval_ref_and_owned(&mut self, i1: &D, mut i2: D) -> D {
        i2.add_assign_by_ref(i1);
        i2
    }

    fn eval_owned(&mut self, i1: D, i2: D) -> D {
        i1 + i2
    }

    fn input_preference(&self) -> (OwnershipPreference, OwnershipPreference) {
        (
            OwnershipPreference::PREFER_OWNED,
            OwnershipPreference::PREFER_OWNED,
        )
    }
}

/// Operator that computes the sum of each value in the stream with itself.
// TODO: A more efficient implementation would rely on a specialized trait.
// E.g., doubling weights in a z-set can be done more efficiently than adddin
// it to itself.
pub struct Times2<D> {
    phantom: PhantomData<D>,
}

impl<D> Times2<D> {
    pub const fn new() -> Self {
        Self {
            phantom: PhantomData,
        }
    }
}

impl<D> Operator for Times2<D>
where
    D: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("2x")
    }

    fn clock_start(&mut self) {}
    fn clock_end(&mut self) {}
}

impl<D> UnaryOperator<D, D> for Times2<D>
where
    D: AddByRef + Clone + 'static,
{
    fn eval(&mut self, i: &D) -> D {
        i.add_by_ref(i)
    }

    fn eval_owned(&mut self, i: D) -> D {
        i.add_by_ref(&i)
    }

    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::INDIFFERENT
    }
}

#[cfg(test)]
mod test {
    use crate::{
        algebra::{finite_map::FiniteMap, zset::ZSetHashMap},
        circuit::{
            operator::{Generator, Inspect},
            Circuit, OwnershipPreference, Root,
        },
    };

    #[test]
    fn scalar_plus() {
        let root = Root::build(move |circuit| {
            let mut n = 0;
            let source1 = circuit.add_source(Generator::new(move || {
                let res = n;
                n += 1;
                res
            }));
            let mut n = 100;
            let source2 = circuit.add_source(Generator::new(move || {
                let res = n;
                n -= 1;
                res
            }));
            source1.plus(&source2).inspect(|n| assert_eq!(*n, 100));
        })
        .unwrap();

        for _ in 0..100 {
            root.step().unwrap();
        }
    }

    #[test]
    fn times2() {
        let root = Root::build(move |circuit| {
            let mut n = 0;
            let source = circuit.add_source(Generator::new(move || {
                let res = n;
                n += 1;
                res
            }));
            let mut step = 0;
            source.plus(&source).inspect(move |n| {
                assert_eq!(*n, step * 2);
                step += 1;
            });
        })
        .unwrap();

        for _ in 0..100 {
            root.step().unwrap();
        }
    }

    #[test]
    fn zset_plus() {
        let build_circuit = |circuit: &Circuit<()>| {
            let mut s = ZSetHashMap::new();
            let source1 = circuit.add_source(Generator::new(move || {
                let res = s.clone();
                s.increment(&5, 1);
                res
            }));
            let mut s = ZSetHashMap::new();
            let source2 = circuit.add_source(Generator::new(move || {
                let res = s.clone();
                s.increment(&5, -1);
                res
            }));
            source1
                .plus(&source2)
                .inspect(|s| assert_eq!(s, &ZSetHashMap::new()));
            (source1, source2)
        };

        // Allow `Plus` to consume both streams by value.
        let root = Root::build(move |circuit| {
            build_circuit(circuit);
        })
        .unwrap();

        for _ in 0..100 {
            root.step().unwrap();
        }

        // Only consume source2 by value.
        let root = Root::build(move |circuit| {
            let (source1, _source2) = build_circuit(circuit);
            circuit.add_sink_with_preference(
                Inspect::new(|_| {}),
                &source1,
                OwnershipPreference::STRONGLY_PREFER_OWNED,
            );
        })
        .unwrap();

        for _ in 0..100 {
            root.step().unwrap();
        }

        // Only consume source1 by value.
        let root = Root::build(move |circuit| {
            let (_source1, source2) = build_circuit(circuit);
            circuit.add_sink_with_preference(
                Inspect::new(|_| {}),
                &source2,
                OwnershipPreference::STRONGLY_PREFER_OWNED,
            );
        })
        .unwrap();

        for _ in 0..100 {
            root.step().unwrap();
        }

        // Consume both streams by reference.
        let root = Root::build(move |circuit| {
            let (source1, source2) = build_circuit(circuit);
            circuit.add_sink_with_preference(
                Inspect::new(|_| {}),
                &source1,
                OwnershipPreference::STRONGLY_PREFER_OWNED,
            );
            circuit.add_sink_with_preference(
                Inspect::new(|_| {}),
                &source2,
                OwnershipPreference::STRONGLY_PREFER_OWNED,
            );
        })
        .unwrap();

        for _ in 0..100 {
            root.step().unwrap();
        }
    }
}
