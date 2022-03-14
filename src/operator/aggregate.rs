//! Aggregation operators.

use std::{borrow::Cow, marker::PhantomData};

use crate::{
    algebra::{
        finite_map::KeyProperties, FiniteHashMap, FiniteMap, GroupValue, MapBuilder, ZRingValue,
    },
    circuit::{
        operator_traits::{BinaryOperator, Operator, UnaryOperator},
        Circuit, Scope, Stream,
    },
    operator::{BinaryOperatorAdapter, UnaryOperatorAdapter},
    SharedRef,
};

impl<P, SR> Stream<Circuit<P>, SR>
where
    P: Clone + 'static,
{
    /// Apply [`Aggregate`] operator to `self`.
    pub fn aggregate<K, VI, VO, W, F, O>(&self, f: F) -> Stream<Circuit<P>, O>
    where
        K: KeyProperties,
        VI: 'static,
        SR: SharedRef + 'static,
        <SR as SharedRef>::Target: FiniteMap<K, VI>,
        for<'a> &'a <SR as SharedRef>::Target: IntoIterator<Item = (&'a K, &'a VI)>,
        F: Fn(&K, &VI) -> VO + 'static,
        VO: 'static,
        W: ZRingValue,
        O: Clone + MapBuilder<VO, W> + 'static,
    {
        self.circuit()
            .add_unary_operator(<UnaryOperatorAdapter<O, _>>::new(Aggregate::new(f)), self)
    }

    /// Incremental version of the [`Aggregate`] operator.
    ///
    /// This is equivalent to `self.integrate().aggregate(f).differentiate()`,
    /// but is more efficient.
    pub fn aggregate_incremental<K, VI, VO, W, F, O>(&self, f: F) -> Stream<Circuit<P>, O>
    where
        K: KeyProperties,
        VI: GroupValue,
        SR: SharedRef + 'static,
        <SR as SharedRef>::Target: FiniteMap<K, VI>,
        <SR as SharedRef>::Target: SharedRef<Target = SR::Target>,
        for<'a> &'a <SR as SharedRef>::Target: IntoIterator<Item = (&'a K, &'a VI)>,
        F: Fn(&K, &VI) -> VO + 'static,
        VO: 'static,
        W: ZRingValue,
        O: Clone + MapBuilder<VO, W> + 'static,
    {
        self.circuit().add_binary_operator(
            BinaryOperatorAdapter::new(AggregateIncremental::new(f)),
            self,
            &self.integrate().delay(),
        )
    }

    /// A version of [`Self::aggregate_incremental`] optimized for linear
    /// aggregation functions.
    ///
    /// This method only works for linear aggregation functions `f`, i.e.,
    /// functions that satisfy `f(a+b) = f(a) + f(b)`.  It will produce
    /// incorrect results if `f` is not linear.
    ///
    /// Note that this method adds the value of the key from the input indexed
    /// Z-set to the output Z-set, i.e., given an input key-value pair `(k,
    /// v)`, the output Z-set contains value `(k, f(k, v))`.  In contrast,
    /// [`Self::aggregate_incremental`] does not automatically include key
    /// in the output, since a user-defined aggregation function can be
    /// designed to return the key if necessar.  However,
    /// such an aggregation function can be non-linear (in fact, the plus
    /// operation may not even be defined for its output type).
    pub fn aggregate_linear_incremental<K, VI, VO, W, F, O>(&self, f: F) -> Stream<Circuit<P>, O>
    where
        K: KeyProperties,
        VI: GroupValue,
        SR: SharedRef + 'static,
        <SR as SharedRef>::Target: FiniteMap<K, VI>,
        <SR as SharedRef>::Target: SharedRef<Target = SR::Target>,
        for<'a> &'a <SR as SharedRef>::Target: IntoIterator<Item = (&'a K, &'a VI)>,
        F: Fn(&K, &VI) -> VO + 'static,
        VO: GroupValue,
        W: ZRingValue,
        O: Clone + MapBuilder<(K, VO), W> + 'static,
    {
        let agg_delta: Stream<_, FiniteHashMap<K, VO>> = self.map_values(f);
        agg_delta.aggregate_incremental(|key, agg_val| (key.clone(), agg_val.clone()))
    }

    /// Incremental nested version of the [`Aggregate`] operator.
    ///
    /// This is equivalent to
    /// `self.integrate().integrate_nested().aggregate(f).differentiate_nested.
    /// differentiate()`, but is more efficient.
    pub fn aggregate_incremental_nested<K, VI, VO, W, F, O>(&self, f: F) -> Stream<Circuit<P>, O>
    where
        K: KeyProperties,
        VI: GroupValue,
        SR: SharedRef + 'static,
        <SR as SharedRef>::Target: FiniteMap<K, VI>,
        <SR as SharedRef>::Target: SharedRef<Target = SR::Target>,
        for<'a> &'a <SR as SharedRef>::Target: IntoIterator<Item = (&'a K, &'a VI)>,
        F: Fn(&K, &VI) -> VO + 'static,
        VO: 'static,
        W: ZRingValue,
        O: MapBuilder<VO, W> + GroupValue,
    {
        self.integrate_nested()
            .aggregate_incremental(f)
            .differentiate_nested()
    }

    /// A version of [`Self::aggregate_incremental_nested`] optimized for linear
    /// aggregation functions.
    ///
    /// This method only works for linear aggregation functions `f`, i.e.,
    /// functions that satisfy `f(a+b) = f(a) + f(b)`.  It will produce
    /// incorrect results if `f` is not linear.
    pub fn aggregate_linear_incremental_nested<K, VI, VO, W, F, O>(
        &self,
        f: F,
    ) -> Stream<Circuit<P>, O>
    where
        K: KeyProperties,
        VI: GroupValue,
        SR: SharedRef + 'static,
        <SR as SharedRef>::Target: FiniteMap<K, VI>,
        <SR as SharedRef>::Target: SharedRef<Target = SR::Target>,
        for<'a> &'a <SR as SharedRef>::Target: IntoIterator<Item = (&'a K, &'a VI)>,
        F: Fn(&K, &VI) -> VO + 'static,
        VO: GroupValue,
        W: ZRingValue,
        O: Clone + MapBuilder<(K, VO), W> + GroupValue,
    {
        self.integrate_nested()
            .aggregate_linear_incremental(f)
            .differentiate_nested()
    }
}

/// Aggregate each indexed Z-set in the input stream.
///
/// Values in the input stream are finite maps that map keys of type
/// `K` to values of type `VI`.  The aggregation function `agg_func`
/// maps each key-value pair into an output value of type `VO`.  The
/// output of the operator is a Z-set of type `O` computed as:
/// `Aggregate(i) = Sum_{(k,v) in i}(+1 x agg_func(k,v))`
///
/// # Type arguments
///
/// * `K` - key type in the input map.
/// * `VI` - value type in the input map.  This is typically a Z-set.
/// * `I` - input map type.
/// * `VO` - output type of the aggregation function; value type in the output
///   Z-set.
/// * `W` - weight type in the output Z-set.
/// * `O` - output Z-set type.
pub struct Aggregate<K, VI, I, VO, W, F, O> {
    agg_func: F,
    _type: PhantomData<(K, VI, I, VO, W, O)>,
}

impl<K, VI, I, VO, W, F, O> Aggregate<K, VI, I, VO, W, F, O> {
    pub fn new(agg_func: F) -> Self {
        Self {
            agg_func,
            _type: PhantomData,
        }
    }
}

impl<K, VI, I, VO, W, F, O> Operator for Aggregate<K, VI, I, VO, W, F, O>
where
    K: 'static,
    VI: 'static,
    I: 'static,
    VO: 'static,
    W: 'static,
    F: 'static,
    O: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Aggregate")
    }
    fn clock_start(&mut self, _scope: Scope) {}
    fn clock_end(&mut self, _scope: Scope) {}
}

impl<K, VI, I, VO, W, F, O> UnaryOperator<I, O> for Aggregate<K, VI, I, VO, W, F, O>
where
    K: KeyProperties,
    VI: 'static,
    I: FiniteMap<K, VI>,
    for<'a> &'a I: IntoIterator<Item = (&'a K, &'a VI)>,
    F: Fn(&K, &VI) -> VO + 'static,
    VO: 'static,
    W: ZRingValue,
    O: Clone + MapBuilder<VO, W> + 'static,
{
    fn eval(&mut self, i: &I) -> O {
        let mut result = O::with_capacity(i.support_size());

        for (k, v) in i.into_iter() {
            result.increment_owned((self.agg_func)(k, v), W::one());
        }

        result
    }
}

/// Incremental version of the `Aggregate` operator.
///
/// Takes a stream `a` of changes to relation `A` and a stream with delayed
/// value of `A`: `z^-1(A) = a.integrate().delay()` and computes
/// `integrate(A) - integrate(z^-1(A))` incrementally, by only considering
/// values in the support of `a`.
pub struct AggregateIncremental<K, VI, I, VO, W, F, O> {
    agg_func: F,
    _type: PhantomData<(K, VI, I, VO, W, O)>,
}

impl<K, VI, I, VO, W, F, O> AggregateIncremental<K, VI, I, VO, W, F, O> {
    pub fn new(agg_func: F) -> Self {
        Self {
            agg_func,
            _type: PhantomData,
        }
    }
}

impl<K, VI, I, VO, W, F, O> Operator for AggregateIncremental<K, VI, I, VO, W, F, O>
where
    K: 'static,
    VI: 'static,
    I: 'static,
    VO: 'static,
    W: 'static,
    F: 'static,
    O: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("AggregateIncremental")
    }
    fn clock_start(&mut self, _scope: Scope) {}
    fn clock_end(&mut self, _scope: Scope) {}
}

impl<K, VI, I, VO, W, F, O> BinaryOperator<I, I, O> for AggregateIncremental<K, VI, I, VO, W, F, O>
where
    K: KeyProperties,
    VI: GroupValue,
    I: FiniteMap<K, VI>,
    for<'a> &'a I: IntoIterator<Item = (&'a K, &'a VI)>,
    F: Fn(&K, &VI) -> VO + 'static,
    VO: 'static,
    W: ZRingValue,
    O: Clone + MapBuilder<VO, W> + 'static,
{
    fn eval(&mut self, delta: &I, delayed_integral: &I) -> O {
        let mut result = O::with_capacity(delta.support_size());

        for (k, v) in delta.into_iter() {
            if let Some(old_val) = delayed_integral.get_in_support(k) {
                // Retract the old value of the aggregate.
                result.increment_owned((self.agg_func)(k, old_val), W::one().neg());

                // Insert updated aggregate.
                let new_val = old_val.add_by_ref(v);
                if !new_val.is_zero() {
                    result.increment_owned((self.agg_func)(k, &old_val.add_by_ref(v)), W::one())
                }
            } else {
                result.increment_owned((self.agg_func)(k, v), W::one())
            }
        }

        result
    }
}

#[cfg(test)]
mod test {
    use std::{cell::RefCell, rc::Rc};

    use crate::{
        algebra::{FiniteHashMap, ZSetHashMap},
        circuit::{Root, Stream},
        finite_map,
        operator::{Apply2, GeneratorNested},
    };

    #[test]
    fn aggregate_test() {
        let root = Root::build(move |circuit| {
            let mut inputs = vec![
                vec![
                    finite_map! { (1, 10) => 1, (1, 20) => 1 },
                    finite_map! { (2, 10) => 1, (1, 10) => -1, (1, 20) => 1, (3, 10) => 1 },
                ],
                vec![
                    finite_map! { (4, 20) => 1, (2, 10) => -1 },
                    finite_map! { (5, 10) => 1, (6, 10) => 1 },
                ],
                vec![],
            ]
            .into_iter();

            circuit
                .iterate(|child| {
                    let counter = Rc::new(RefCell::new(0));
                    let counter_clone = counter.clone();

                    let input: Stream<_, FiniteHashMap<usize, ZSetHashMap<usize, isize>>> = child
                        .add_source(GeneratorNested::new(Box::new(move || {
                            *counter_clone.borrow_mut() = 0;
                            let mut deltas =
                                inputs.next().unwrap_or_else(|| Vec::new()).into_iter();
                            Box::new(move || deltas.next().unwrap_or_else(|| finite_map! {}))
                        })))
                        .index();

                    // Weighted sum aggregate.  Returns `(key, weighted_sum)`.
                    let sum = |key: &usize, zset: &ZSetHashMap<usize, isize>| -> (usize, isize) {
                        let mut result: isize = 0;
                        for (v, w) in zset.into_iter() {
                            result += (*v as isize) * w;
                        }

                        (key.clone(), result)
                    };

                    // Weighted sum aggregate that returns only the weighted sum
                    // value and is therefore linear.
                    let sum_linear = |_key: &usize, zset: &ZSetHashMap<usize, isize>| -> isize {
                        let mut result: isize = 0;
                        for (v, w) in zset.into_iter() {
                            result += (*v as isize) * w;
                        }

                        result
                    };

                    let sum_inc = input.aggregate_incremental_nested(sum.clone());
                    let sum_inc_linear = input.aggregate_linear_incremental_nested(sum_linear);
                    let sum_noninc = input
                        .integrate_nested()
                        .integrate()
                        .aggregate(sum)
                        .differentiate()
                        .differentiate_nested();

                    // Compare outputs of all three implementations.
                    child
                        .add_binary_operator(
                            Apply2::new(
                                |d1: &FiniteHashMap<(usize, isize), isize>,
                                 d2: &FiniteHashMap<(usize, isize), isize>| {
                                    (d1.clone(), d2.clone())
                                },
                            ),
                            &sum_inc,
                            &sum_noninc,
                        )
                        .inspect(|(d1, d2)| {
                            //println!("incremental: {:?}", d1);
                            //println!("non-incremental: {:?}", d2);
                            assert_eq!(d1, d2);
                        });

                    child
                        .add_binary_operator(
                            Apply2::new(
                                |d1: &FiniteHashMap<(usize, isize), isize>,
                                 d2: &FiniteHashMap<(usize, isize), isize>| {
                                    (d1.clone(), d2.clone())
                                },
                            ),
                            &sum_inc,
                            &sum_inc_linear,
                        )
                        .inspect(|(d1, d2)| {
                            assert_eq!(d1, d2);
                        });

                    // Min aggregate (non-linear).
                    let min = |key: &usize, zset: &ZSetHashMap<usize, isize>| -> (usize, usize) {
                        let mut result: usize = *zset.into_iter().next().unwrap().0;
                        for (&v, _) in zset.into_iter() {
                            if v < result {
                                result = v;
                            }
                        }

                        (key.clone(), result)
                    };

                    let min_inc = input.aggregate_incremental_nested(min.clone());
                    let min_noninc = input
                        .integrate_nested()
                        .integrate()
                        .aggregate(min)
                        .differentiate()
                        .differentiate_nested();

                    child
                        .add_binary_operator(
                            Apply2::new(
                                |d1: &FiniteHashMap<(usize, usize), isize>,
                                 d2: &FiniteHashMap<(usize, usize), isize>| {
                                    (d1.clone(), d2.clone())
                                },
                            ),
                            &min_inc,
                            &min_noninc,
                        )
                        .inspect(|(d1, d2)| {
                            assert_eq!(d1, d2);
                        });

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
