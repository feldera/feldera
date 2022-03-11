//! Distinct operator.

use std::{borrow::Cow, marker::PhantomData};

use crate::{
    algebra::{finite_map::KeyProperties, ZRingValue, ZSet},
    circuit::{
        operator_traits::{BinaryOperator, Operator, UnaryOperator},
        Circuit, NodeId, Scope, Stream,
    },
    circuit_cache_key,
    operator::{BinaryOperatorAdapter, UnaryOperatorAdapter},
    SharedRef,
};

circuit_cache_key!(DistinctId<C, D>(NodeId => Stream<C, D>));
circuit_cache_key!(DistinctIncrementalId<C, D>(NodeId => Stream<C, D>));

impl<P, Z> Stream<Circuit<P>, Z>
where
    P: Clone + 'static,
{
    /// Apply [`Distinct`] operator to `self`.
    pub fn distinct<V, W>(&self) -> Stream<Circuit<P>, <Z as SharedRef>::Target>
    where
        V: KeyProperties,
        W: ZRingValue,
        Z: SharedRef + 'static,
        <Z as SharedRef>::Target: ZSet<V, W>,
    {
        self.circuit()
            .cache_get_or_insert_with(DistinctId::new(self.local_node_id()), || {
                self.circuit()
                    .add_unary_operator(UnaryOperatorAdapter::new(Distinct::new()), self)
            })
            .clone()
    }

    /// Incremental version of the [`Distinct`] operator.
    ///
    /// This is equivalent to `self.integrate().distinct().differentiate()`, but
    /// is more efficient.
    pub fn distinct_incremental<V, W>(&self) -> Stream<Circuit<P>, <Z as SharedRef>::Target>
    where
        V: KeyProperties,
        W: ZRingValue,
        Z: SharedRef + 'static,
        <Z as SharedRef>::Target: ZSet<V, W> + SharedRef<Target = Z::Target>,
        for<'a> &'a <Z as SharedRef>::Target: IntoIterator<Item = (&'a V, &'a W)>,
    {
        self.circuit()
            .cache_get_or_insert_with(DistinctIncrementalId::new(self.local_node_id()), || {
                self.circuit().add_binary_operator(
                    BinaryOperatorAdapter::new(DistinctIncremental::new()),
                    self,
                    &self.integrate().delay(),
                )
            })
            .clone()
    }

    /// Incremental nested version of the [`Distinct`] operator.
    pub fn distinct_incremental_nested<V, W>(&self) -> Stream<Circuit<P>, <Z as SharedRef>::Target>
    where
        V: KeyProperties,
        W: ZRingValue,
        Z: SharedRef + 'static,
        <Z as SharedRef>::Target: ZSet<V, W> + SharedRef<Target = Z::Target>,
        for<'a> &'a <Z as SharedRef>::Target: IntoIterator<Item = (&'a V, &'a W)>,
    {
        self.integrate_nested()
            .distinct_incremental()
            .differentiate_nested()
    }
}

/// Distinct operator changes all weights in the support of a Z-set to 1.
pub struct Distinct<V, W, Z> {
    _type: PhantomData<(V, W, Z)>,
}

impl<V, W, Z> Distinct<V, W, Z> {
    pub fn new() -> Self {
        Self { _type: PhantomData }
    }
}

impl<V, W, Z> Default for Distinct<V, W, Z> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V, W, Z> Operator for Distinct<V, W, Z>
where
    V: 'static,
    W: 'static,
    Z: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Distinct")
    }
    fn clock_start(&mut self, _scope: Scope) {}
    fn clock_end(&mut self, _scope: Scope) {}
}

impl<V, W, Z> UnaryOperator<Z, Z> for Distinct<V, W, Z>
where
    V: KeyProperties,
    W: ZRingValue,
    Z: ZSet<V, W> + 'static,
{
    fn eval(&mut self, i: &Z) -> Z {
        i.distinct()
    }

    fn eval_owned(&mut self, i: Z) -> Z {
        i.distinct_owned()
    }
}

/// Incremental version of the distinct operator.
///
/// Takes a stream `a` of changes to relation `A` and a stream with delayed
/// value of `A`: `z^-1(A) = a.integrate().delay()` and computes
/// `distinct(A) - distinct(z^-1(A))` incrementally, by only considering
/// values in the support of `a`.
struct DistinctIncremental<V, W, Z> {
    _type: PhantomData<(V, W, Z)>,
}

impl<V, W, Z> DistinctIncremental<V, W, Z> {
    pub fn new() -> Self {
        Self { _type: PhantomData }
    }
}

impl<V, W, Z> Default for DistinctIncremental<V, W, Z> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V, W, Z> Operator for DistinctIncremental<V, W, Z>
where
    V: 'static,
    W: 'static,
    Z: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("DistinctIncremental")
    }
    fn clock_start(&mut self, _scope: Scope) {}
    fn clock_end(&mut self, _scope: Scope) {}
}

impl<V, W, Z> BinaryOperator<Z, Z, Z> for DistinctIncremental<V, W, Z>
where
    V: KeyProperties,
    W: ZRingValue,
    for<'a> &'a Z: IntoIterator<Item = (&'a V, &'a W)>,
    Z: ZSet<V, W> + 'static,
{
    fn eval(&mut self, delta: &Z, delayed_integral: &Z) -> Z {
        let mut result = Z::empty();

        for (v, w) in delta.into_iter() {
            let old_weight = delayed_integral.lookup(v);
            let new_weight = old_weight.clone() + w.clone();

            if old_weight.le0() {
                // Weight changes from non-positive to positive.
                if new_weight.ge0() && !new_weight.is_zero() {
                    result.increment(v, W::one());
                }
            } else if new_weight.le0() {
                // Weight changes from positive to non-positive.
                result.increment(v, W::one().neg());
            }
        }

        result
    }

    fn eval_owned_and_ref(&mut self, delta: Z, delayed_integral: &Z) -> Z {
        let mut result = Z::empty();

        for (v, w) in delta.into_iter() {
            let old_weight = delayed_integral.lookup(&v);
            let new_weight = old_weight.clone() + w;

            if old_weight.le0() {
                // Weight changes from non-positive to positive.
                if new_weight.ge0() && !new_weight.is_zero() {
                    result.increment_owned(v, W::one());
                }
            } else if new_weight.le0() {
                // Weight changes from positive to non-positive.
                result.increment_owned(v, W::one().neg());
            }
        }

        result
    }

    fn eval_owned(&mut self, delta: Z, delayed_integral: Z) -> Z {
        self.eval_owned_and_ref(delta, &delayed_integral)
    }
}

#[cfg(test)]
mod test {
    use std::{cell::RefCell, rc::Rc};

    use crate::{
        algebra::FiniteHashMap,
        circuit::Root,
        finite_map,
        operator::{Apply2, GeneratorNested},
    };

    #[test]
    fn distinct_incremental_nested_test() {
        let root = Root::build(move |circuit| {
            let mut inputs = vec![
                vec![
                    finite_map! { 1 => 1, 2 => 1 },
                    finite_map! { 2 => -1, 3 => 2, 4 => 2 },
                ],
                vec![
                    finite_map! { 2 => 1, 3 => 1 },
                    finite_map! { 3 => -2, 4 => -1 },
                ],
                vec![
                    finite_map! { 5 => 1, 6 => 1 },
                    finite_map! { 2 => -1, 7 => 1 },
                    finite_map! { 2 => 1, 7 => -1, 8 => 2, 9 => 1 },
                ],
            ]
            .into_iter();

            circuit
                .iterate(|child| {
                    let counter = Rc::new(RefCell::new(0));
                    let counter_clone = counter.clone();

                    let input = child.add_source(GeneratorNested::new(Box::new(move || {
                        *counter_clone.borrow_mut() = 0;
                        let mut deltas = inputs.next().unwrap_or_else(|| Vec::new()).into_iter();
                        Box::new(move || deltas.next().unwrap_or_else(|| finite_map! {}))
                    })));

                    let distinct_inc = input.distinct_incremental_nested();
                    let distinct_noninc = input
                        // Non-incremental implementation of distinct_nested_incremental.
                        .integrate()
                        .integrate_nested()
                        .distinct()
                        .differentiate()
                        .differentiate_nested();

                    child
                        .add_binary_operator(
                            Apply2::new(
                                |d1: &FiniteHashMap<usize, isize>,
                                 d2: &FiniteHashMap<usize, isize>| {
                                    (d1.clone(), d2.clone())
                                },
                            ),
                            &distinct_inc,
                            &distinct_noninc,
                        )
                        .inspect(|(d1, d2)| assert_eq!(d1, d2));

                    Ok((
                        move || {
                            *counter.borrow_mut() += 1;
                            *counter.borrow() == 4
                        },
                        (),
                    ))
                })
                .unwrap();
        })
        .unwrap();

        for _ in 0..3 {
            root.step().unwrap();
        }
    }
}
