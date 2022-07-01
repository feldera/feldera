//! Aggregation operators.

use std::{borrow::Cow, marker::PhantomData, ops::Neg};

use crate::{
    algebra::{HasOne, IndexedZSet, ZRingValue, ZSet},
    circuit::{
        operator_traits::{BinaryOperator, Operator, UnaryOperator},
        Circuit, Scope, Stream,
    },
    trace::{cursor::Cursor, BatchReader},
    NumEntries,
};
use deepsize::DeepSizeOf;

impl<P, Z> Stream<Circuit<P>, Z>
where
    P: Clone + 'static,
    Z: Clone + 'static,
{
    // TODO: Consider changing the signature of aggregation function to take a slice
    // of values instead of iterator.  This is easier to understand and use, and
    // allows computing the number of unique values in a group (an important
    // aggregate) in `O(1)`.  Most batch implementations will allow extracting
    // such a slice efficiently.
    /// Aggregate each indexed Z-set in the input stream.
    ///
    /// Values in the input stream are
    /// [indexed Z-sets](`crate::algebra::IndexedZSet`). The aggregation
    /// function `agg_func` takes a single key and a sorted array of (value,
    /// weight) tuples associated with this key and transforms them into a
    /// single aggregate value.  The output of the operator is a Z-set
    /// computed as a sum of aggregates across all keys with weight `+1`
    /// each.
    ///
    /// # Type arguments
    ///
    /// * `Z` - input indexed Z-set type.
    /// * `O` - output Z-set type.
    pub fn aggregate<F, O>(&self, f: F) -> Stream<Circuit<P>, O>
    where
        Z: IndexedZSet<R = O::R> + 'static,
        F: Fn(&Z::Key, &mut Vec<(&Z::Val, Z::R)>) -> O::Key + 'static,
        O: Clone + ZSet + 'static,
        O::R: ZRingValue,
    {
        self.circuit().add_unary_operator(Aggregate::new(f), self)
    }

    /// Incremental version of the [`Aggregate`] operator.
    ///
    /// This is equivalent to `self.integrate().aggregate(f).differentiate()`,
    /// but is more efficient.
    pub fn aggregate_incremental<F, O>(&self, f: F) -> Stream<Circuit<P>, O>
    where
        Z: IndexedZSet<R = O::R> + DeepSizeOf + NumEntries,
        Z::Key: PartialEq + Ord,
        Z::Val: Ord,
        F: Fn(&Z::Key, &mut Vec<(&Z::Val, Z::R)>) -> O::Key + Clone + 'static,
        O: Clone + ZSet + 'static,
        O::R: ZRingValue,
    {
        // Retract old values of the aggregate for affected keys.
        let retract_old = self.circuit().add_binary_operator(
            AggregateIncremental::new(false, f.clone()),
            self,
            &self.integrate_trace().delay_trace(),
        );

        // Insert new aggregates.
        let insert_new = self.circuit().add_binary_operator(
            AggregateIncremental::new(true, f),
            self,
            &self.integrate_trace(),
        );

        retract_old.plus(&insert_new)
    }

    /// Incremental nested version of the [`Aggregate`] operator.
    ///
    /// This is equivalent to
    /// `self.integrate().integrate_nested().aggregate(f).differentiate_nested.
    /// differentiate()`, but is more efficient.
    pub fn aggregate_incremental_nested<F, O>(&self, f: F) -> Stream<Circuit<P>, O>
    where
        Z: IndexedZSet<R = O::R> + DeepSizeOf + NumEntries,
        Z::Key: PartialEq + Ord,
        Z::Val: Ord,
        F: Fn(&Z::Key, &mut Vec<(&Z::Val, Z::R)>) -> O::Key + Clone + 'static,
        O: Clone + ZSet + DeepSizeOf + 'static,
        O::R: ZRingValue,
    {
        self.integrate_nested()
            .aggregate_incremental(f)
            .differentiate_nested()
    }

    /*
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
    pub fn aggregate_linear_incremental<F, O>(&self, f: F) -> Stream<Circuit<P>, O>
    where
        <SR as SharedRef>::Target: BatchReader<R=O::R> + DeepSizeOf + NumEntries + GroupValue + SharedRef<Target = SR::Target> + 'static,
        <<SR as SharedRef>::Target as BatchReader>::Key: PartialEq + Clone,
        F: Fn(&<<SR as SharedRef>::Target as BatchReader>::Key,
              &<<SR as SharedRef>::Target as BatchReader>::Val) -> O::Val + 'static,
        O: Clone + ZSet + 'static,
    {
        let agg_delta: Stream<_, OrdZSet<_, _>> = self.map_values(f);
        agg_delta.aggregate_incremental(|zset, cursor| (zset.key().clone(), agg_val.clone()))
    }
    */

    /*
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
        <SR as SharedRef>::Target: ZSet<K, VI>,
        <SR as SharedRef>::Target: NumEntries + SharedRef<Target = SR::Target>,
        for<'a> &'a <SR as SharedRef>::Target: IntoIterator<Item = (&'a K, &'a VI)>,
        F: Fn(&K, &VI) -> VO + 'static,
        VO: NumEntries + GroupValue,
        W: ZRingValue,
        O: Clone + MapBuilder<(K, VO), W> + NumEntries + GroupValue,
    {
        self.integrate_nested()
            .aggregate_linear_incremental(f)
            .differentiate_nested()
    }
    */
}

pub struct Aggregate<Z, F, O> {
    agg_func: F,
    _type: PhantomData<(Z, O)>,
}

impl<Z, F, O> Aggregate<Z, F, O> {
    pub fn new(agg_func: F) -> Self {
        Self {
            agg_func,
            _type: PhantomData,
        }
    }
}

impl<Z, F, O> Operator for Aggregate<Z, F, O>
where
    Z: 'static,
    F: 'static,
    O: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Aggregate")
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<Z, F, O> UnaryOperator<Z, O> for Aggregate<Z, F, O>
where
    Z: IndexedZSet<R = O::R> + 'static,
    F: Fn(&Z::Key, &mut Vec<(&Z::Val, Z::R)>) -> O::Key + 'static,
    O: Clone + ZSet + 'static,
    O::R: ZRingValue,
{
    fn eval(&mut self, i: &Z) -> O {
        let mut elements = Vec::with_capacity(i.len());
        let mut cursor = i.cursor();
        let mut vals: Vec<(&Z::Val, Z::R)> = Vec::with_capacity(i.len());

        while cursor.key_valid() {
            cursor.values(&mut vals);
            // Skip keys that only contain values with weight zero.
            if !vals.is_empty() {
                elements.push((((self.agg_func)(cursor.key(), &mut vals), ()), Z::R::one()));
            }
            vals.clear();
            cursor.step_key();
        }
        O::from_tuples((), elements)
    }
}

/// Incremental version of the `Aggregate` operator.
///
/// Takes a stream `a` of changes to relation `A` and a stream with delayed
/// value of `A`: `z^-1(A) = a.integrate().delay()` and computes
/// `integrate(A) - integrate(z^-1(A))` incrementally, by only considering
/// values in the support of `a`.
pub struct AggregateIncremental<Z, I, F, O> {
    polarity: bool,
    agg_func: F,
    _type: PhantomData<(Z, I, O)>,
}

impl<Z, I, F, O> AggregateIncremental<Z, I, F, O> {
    pub fn new(polarity: bool, agg_func: F) -> Self {
        Self {
            polarity,
            agg_func,
            _type: PhantomData,
        }
    }
}

impl<Z, I, F, O> Operator for AggregateIncremental<Z, I, F, O>
where
    Z: 'static,
    I: 'static,
    F: 'static,
    O: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("AggregateIncremental")
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<Z, I, F, O> BinaryOperator<Z, I, O> for AggregateIncremental<Z, I, F, O>
where
    Z: IndexedZSet<R = O::R> + 'static,
    Z::Key: PartialEq,
    I: BatchReader<Key = Z::Key, Val = Z::Val, Time = (), R = O::R> + 'static,
    F: Fn(&Z::Key, &mut Vec<(&Z::Val, Z::R)>) -> O::Key + 'static,
    O: Clone + ZSet + 'static,
    O::R: ZRingValue,
{
    fn eval(&mut self, delta: &Z, integral: &I) -> O {
        let mut result = Vec::with_capacity(delta.len());

        let mut delta_cursor = delta.cursor();
        let mut integral_cursor = integral.cursor();
        let weight = if self.polarity {
            I::R::one()
        } else {
            I::R::one().neg()
        };

        // This could be an overkill.
        let mut vals: Vec<(&Z::Val, Z::R)> = Vec::with_capacity(delta.len());

        while delta_cursor.key_valid() {
            let key = delta_cursor.key();

            integral_cursor.seek_key(key);

            if integral_cursor.key_valid() && integral_cursor.key() == key {
                integral_cursor.values(&mut vals);
                // Skip keys that only contain values with weight 0.
                if !vals.is_empty() {
                    result.push((((self.agg_func)(key, &mut vals), ()), weight.clone()));
                }
                vals.clear();
            }
            delta_cursor.step_key();
        }

        O::from_tuples((), result)
    }
}

#[cfg(test)]
mod test {
    use std::{cell::RefCell, rc::Rc};

    use crate::{
        circuit::Root,
        operator::{Apply2, GeneratorNested},
        trace::ord::OrdZSet,
        zset,
    };

    #[test]
    fn aggregate_test() {
        let root = Root::build(move |circuit| {
            let mut inputs = vec![
                vec![
                    zset! { (1, 10) => 1, (1, 20) => 1 },
                    zset! { (2, 10) => 1, (1, 10) => -1, (1, 20) => 1, (3, 10) => 1 },
                ],
                vec![
                    zset! { (4, 20) => 1, (2, 10) => -1 },
                    zset! { (5, 10) => 1, (6, 10) => 1 },
                ],
                vec![],
            ]
            .into_iter();

            circuit
                .iterate(|child| {
                    let counter = Rc::new(RefCell::new(0));
                    let counter_clone = counter.clone();

                    let input = child
                        .add_source(GeneratorNested::new(Box::new(move || {
                            *counter_clone.borrow_mut() = 0;
                            let mut deltas = inputs.next().unwrap_or_default().into_iter();
                            Box::new(move || deltas.next().unwrap_or_else(|| zset! {}))
                        })))
                        .index();

                    // Weighted sum aggregate.  Returns `(key, weighted_sum)`.
                    let sum = |key: &usize, vals: &mut Vec<(&usize, isize)>| -> (usize, isize) {
                        let result: isize = vals.drain(..).map(|(v, w)| (*v as isize) * w).sum();
                        (*key, result)
                    };

                    // Weighted sum aggregate that returns only the weighted sum
                    // value and is therefore linear.
                    /*let sum_linear = |_key: &usize, zset: &OrdZSet<usize, isize>| -> isize {
                        let mut result: isize = 0;
                        for (v, w) in zset.into_iter() {
                            result += (*v as isize) * w;
                        }

                        result
                    };*/

                    let sum_inc = input.aggregate_incremental_nested(sum);
                    //let sum_inc_linear = input.aggregate_linear_incremental_nested(sum_linear);
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
                                |d1: &OrdZSet<(usize, isize), isize>,
                                 d2: &OrdZSet<(usize, isize), isize>| {
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

                    /*child
                    .add_binary_operator(
                        Apply2::new(
                            |d1: &OrdZSet<(usize, isize), isize>,
                             d2: &OrdZSet<(usize, isize), isize>| {
                                (d1.clone(), d2.clone())
                            },
                        ),
                        &sum_inc,
                        &sum_inc_linear,
                    )
                    .inspect(|(d1, d2)| {
                        assert_eq!(d1, d2);
                    });*/

                    // Min aggregate (non-linear).
                    let min = |&key: &usize, vals: &mut Vec<(&usize, isize)>| -> (usize, usize) {
                        let result = vals
                            .drain(..)
                            .map(|(&value, _)| value)
                            .min()
                            .unwrap_or(usize::MAX);

                        (key, result)
                    };

                    let min_inc = input.aggregate_incremental_nested(min);
                    let min_noninc = input
                        .integrate_nested()
                        .integrate()
                        .aggregate(min)
                        .differentiate()
                        .differentiate_nested();

                    child
                        .add_binary_operator(
                            Apply2::new(
                                |d1: &OrdZSet<(usize, usize), isize>,
                                 d2: &OrdZSet<(usize, usize), isize>| {
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
