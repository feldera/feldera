//! N-ary plus operator.

use crate::{
    algebra::{AddAssignByRef, AddByRef, HasZero, WithSize},
    circuit::{
        operator_traits::{NaryOperator, Operator},
        Circuit, OwnershipPreference, Scope, Stream,
    },
};
use std::{borrow::Cow, iter::once, mem::transmute, ops::Add};

impl<P, D> Stream<Circuit<P>, D>
where
    P: Clone + 'static,
    D: Add<Output = D> + AddByRef + AddAssignByRef + Clone + HasZero + WithSize + 'static,
{
    /// Apply the [`Sum`] operator to `self` and all streams in `streams`.
    pub fn sum<'a, I>(&'a self, streams: I) -> Stream<Circuit<P>, D>
    where
        I: IntoIterator<Item = &'a Self>,
    {
        self.circuit()
            .add_nary_operator(Sum::new(), once(self).chain(streams))
    }
}

/// Operator that computes the sum of values across all its input streams at
/// each timestamp.
pub struct Sum<D>
where
    D: Clone + 'static,
{
    // Vector of input values.  Keep it here to reuse the allocation across
    // operator invocations.
    inputs: Vec<Cow<'static, D>>,
}

impl<D> Sum<D>
where
    D: Clone + 'static,
{
    pub fn new() -> Self {
        Self { inputs: Vec::new() }
    }
}

impl<D> Default for Sum<D>
where
    D: Clone + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<D> Operator for Sum<D>
where
    D: Clone + 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Sum")
    }

    fn clock_start(&mut self, _scope: Scope) {}
    fn clock_end(&mut self, _scope: Scope) {}
}

impl<D> NaryOperator<D, D> for Sum<D>
where
    D: Add<Output = D> + AddAssignByRef + Clone + HasZero + WithSize + 'static,
{
    fn eval<'a, Iter>(&mut self, inputs: Iter) -> D
    where
        Iter: Iterator<Item = Cow<'a, D>>,
    {
        let input_vec = unsafe {
            transmute::<&mut Vec<Cow<'static, D>>, &mut Vec<Cow<'a, D>>>(&mut self.inputs)
        };
        input_vec.clear();
        for input in inputs {
            input_vec.push(input);
        }
        input_vec.sort_by(|x, y| y.size().cmp(&x.size()));

        if input_vec.is_empty() {
            return D::zero();
        }
        let mut res = D::zero();
        for input in input_vec.drain(..) {
            if res.is_zero() {
                res = match input {
                    Cow::Borrowed(v) => v.clone(),
                    Cow::Owned(v) => v,
                };
            } else {
                match input {
                    Cow::Borrowed(v) => res.add_assign_by_ref(v),
                    Cow::Owned(v) => {
                        res = res + v;
                    }
                }
            }
        }

        res
    }

    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::PREFER_OWNED
    }
}

#[cfg(test)]
mod test {
    use crate::{
        algebra::{MapBuilder, ZSetHashMap},
        circuit::{Circuit, OwnershipPreference, Root},
        operator::{Generator, Inspect},
    };

    #[test]
    fn zset_sum() {
        let build_circuit = |circuit: &Circuit<()>| {
            let mut s = ZSetHashMap::new();
            let source1 = circuit.add_source(Generator::new(move || {
                let res = s.clone();
                s.increment(&5, 1);
                s.increment(&6, 2);
                res
            }));
            let mut s = ZSetHashMap::new();
            let source2 = circuit.add_source(Generator::new(move || {
                let res = s.clone();
                s.increment(&5, -1);
                res
            }));
            let mut s = ZSetHashMap::new();
            let source3 = circuit.add_source(Generator::new(move || {
                let res = s.clone();
                s.increment(&6, -1);
                res
            }));

            // Supply `source3` twice to test the handling of aliases.
            source3
                .sum(&[source2.clone(), source1.clone(), source3.clone()])
                .inspect(|s| assert_eq!(s, &ZSetHashMap::new()));
            (source1, source2, source3)
        };

        // Allow `Sum` to consume all streams by value.
        let root = Root::build(move |circuit| {
            build_circuit(circuit);
        })
        .unwrap();

        for _ in 0..100 {
            root.step().unwrap();
        }

        // Only consume source2, source3 by value.
        let root = Root::build(move |circuit| {
            let (source1, _source2, _source3) = build_circuit(circuit);
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

        // Consume all streams by reference.
        let root = Root::build(move |circuit| {
            let (source1, source2, source3) = build_circuit(circuit);
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
            circuit.add_sink_with_preference(
                Inspect::new(|_| {}),
                &source3,
                OwnershipPreference::STRONGLY_PREFER_OWNED,
            );
        })
        .unwrap();

        for _ in 0..100 {
            root.step().unwrap();
        }
    }
}
