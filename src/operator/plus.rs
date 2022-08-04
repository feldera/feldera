//! Binary plus and minus operators.

use crate::{
    algebra::{AddAssignByRef, AddByRef, NegByRef},
    circuit::{
        operator_traits::{BinaryOperator, Operator},
        Circuit, OwnershipPreference, Scope, Stream,
    },
};
use std::{
    borrow::Cow,
    marker::PhantomData,
    ops::{Add, Neg},
};

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
    /// # use dbsp::{
    /// #   operator::Generator,
    /// #   Circuit,
    /// # };
    /// let circuit = Circuit::build(move |circuit| {
    ///     // Stream of non-negative values: 0, 1, 2, ...
    ///     let mut n = 0;
    ///     let source1 = circuit.add_source(Generator::new(move || {
    ///         let res = n;
    ///         n += 1;
    ///         res
    ///     }));
    ///     // Stream of non-positive values: 0, -1, -2, ...
    ///     let mut n = 0;
    ///     let source2 = circuit.add_source(Generator::new(move || {
    ///         let res = n;
    ///         n -= 1;
    ///         res
    ///     }));
    ///     // Compute pairwise sums of values in the stream; the output stream will contain zeros.
    ///     source1.plus(&source2).inspect(|n| assert_eq!(*n, 0));
    /// })
    /// .unwrap()
    /// .0;
    ///
    /// # for _ in 0..5 {
    /// #     circuit.step().unwrap();
    /// # }
    /// ```
    pub fn plus(&self, other: &Stream<Circuit<P>, D>) -> Stream<Circuit<P>, D> {
        self.circuit().add_binary_operator(Plus::new(), self, other)
    }
}

impl<P, D> Stream<Circuit<P>, D>
where
    P: Clone + 'static,
    D: Add<Output = D> + AddByRef + AddAssignByRef + Neg<Output = D> + NegByRef + Clone + 'static,
{
    /// Apply the [`Minus`] operator to `self` and `other`.
    pub fn minus(&self, other: &Stream<Circuit<P>, D>) -> Stream<Circuit<P>, D> {
        self.circuit()
            .add_binary_operator(Minus::new(), self, other)
    }
}

/// Operator that computes the sum of values in its two input streams at each
/// timestamp.
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

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
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

/// Operator that computes the difference of values in its two input streams at
/// each timestamp.
pub struct Minus<D> {
    phantom: PhantomData<D>,
}

impl<D> Minus<D> {
    pub const fn new() -> Self {
        Self {
            phantom: PhantomData,
        }
    }
}

impl<D> Operator for Minus<D>
where
    D: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Minus")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

// TODO: Add `subtract` operation to `GroupValue`, which
// can be more efficient than negate followed by plus.
impl<D> BinaryOperator<D, D, D> for Minus<D>
where
    D: Add<Output = D> + AddByRef + AddAssignByRef + Neg<Output = D> + NegByRef + Clone + 'static,
{
    fn eval(&mut self, i1: &D, i2: &D) -> D {
        let mut i2neg = i2.neg_by_ref();
        i2neg.add_assign_by_ref(i1);
        i2neg
    }

    fn eval_owned_and_ref(&mut self, i1: D, i2: &D) -> D {
        i1.add(i2.neg_by_ref())
    }

    fn eval_ref_and_owned(&mut self, i1: &D, i2: D) -> D {
        i2.neg().add_by_ref(i1)
    }

    fn eval_owned(&mut self, i1: D, i2: D) -> D {
        i1 + i2.neg()
    }

    fn input_preference(&self) -> (OwnershipPreference, OwnershipPreference) {
        (
            OwnershipPreference::PREFER_OWNED,
            OwnershipPreference::PREFER_OWNED,
        )
    }
}

#[cfg(test)]
mod test {
    use crate::{
        algebra::HasZero,
        circuit::OwnershipPreference,
        operator::{Generator, Inspect},
        trace::{ord::OrdZSet, Batch},
        zset, Circuit,
    };

    #[test]
    fn scalar_plus() {
        let circuit = Circuit::build(move |circuit| {
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
        .unwrap()
        .0;

        for _ in 0..100 {
            circuit.step().unwrap();
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn zset_plus() {
        let build_plus_circuit = |circuit: &Circuit<()>| {
            let mut s = <OrdZSet<_, _>>::zero();
            let delta = zset! { 5 => 1};
            let source1 = circuit.add_source(Generator::new(move || {
                s = s.merge(&delta);
                s.clone()
            }));
            let mut s = <OrdZSet<_, _>>::zero();
            let delta = zset! { 5 => -1};
            let source2 = circuit.add_source(Generator::new(move || {
                s = s.merge(&delta);
                s.clone()
            }));
            source1
                .plus(&source2)
                .inspect(|s| assert_eq!(s, &<OrdZSet<usize, isize>>::zero()));
            (source1, source2)
        };

        let build_minus_circuit = |circuit: &Circuit<()>| {
            let mut s = <OrdZSet<_, _>>::zero();
            let delta = zset! { 5 => 1};
            let source1 = circuit.add_source(Generator::new(move || {
                s = s.merge(&delta);
                s.clone()
            }));
            let mut s = <OrdZSet<_, _>>::zero();
            let delta = zset! { 5 => 1};
            let source2 = circuit.add_source(Generator::new(move || {
                s = s.merge(&delta);
                s.clone()
            }));
            source1
                .minus(&source2)
                .inspect(|s| assert_eq!(s, &<OrdZSet<_, _>>::zero()));
            (source1, source2)
        };
        // Allow `Plus` to consume both streams by value.
        let circuit = Circuit::build(move |circuit| {
            build_plus_circuit(circuit);
            build_minus_circuit(circuit);
        })
        .unwrap()
        .0;

        for _ in 0..100 {
            circuit.step().unwrap();
        }

        // Only consume source2 by value.
        let circuit = Circuit::build(move |circuit| {
            let (source1, _source2) = build_plus_circuit(circuit);
            circuit.add_unary_operator_with_preference(
                Inspect::new(|_| {}),
                &source1,
                OwnershipPreference::STRONGLY_PREFER_OWNED,
            );
            let (source3, _source4) = build_minus_circuit(circuit);
            circuit.add_unary_operator_with_preference(
                Inspect::new(|_| {}),
                &source3,
                OwnershipPreference::STRONGLY_PREFER_OWNED,
            );
        })
        .unwrap()
        .0;

        for _ in 0..100 {
            circuit.step().unwrap();
        }

        // Only consume source1 by value.
        let circuit = Circuit::build(move |circuit| {
            let (_source1, source2) = build_plus_circuit(circuit);
            circuit.add_unary_operator_with_preference(
                Inspect::new(|_| {}),
                &source2,
                OwnershipPreference::STRONGLY_PREFER_OWNED,
            );

            let (_source3, source4) = build_minus_circuit(circuit);
            circuit.add_unary_operator_with_preference(
                Inspect::new(|_| {}),
                &source4,
                OwnershipPreference::STRONGLY_PREFER_OWNED,
            );
        })
        .unwrap()
        .0;

        for _ in 0..100 {
            circuit.step().unwrap();
        }

        // Consume both streams by reference.
        let circuit = Circuit::build(move |circuit| {
            let (source1, source2) = build_plus_circuit(circuit);
            circuit.add_unary_operator_with_preference(
                Inspect::new(|_| {}),
                &source1,
                OwnershipPreference::STRONGLY_PREFER_OWNED,
            );
            circuit.add_unary_operator_with_preference(
                Inspect::new(|_| {}),
                &source2,
                OwnershipPreference::STRONGLY_PREFER_OWNED,
            );

            let (source3, source4) = build_minus_circuit(circuit);
            circuit.add_unary_operator_with_preference(
                Inspect::new(|_| {}),
                &source3,
                OwnershipPreference::STRONGLY_PREFER_OWNED,
            );
            circuit.add_unary_operator_with_preference(
                Inspect::new(|_| {}),
                &source4,
                OwnershipPreference::STRONGLY_PREFER_OWNED,
            );
        })
        .unwrap()
        .0;

        for _ in 0..100 {
            circuit.step().unwrap();
        }
    }
}
