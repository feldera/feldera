//! Relational join operator.

use crate::{
    algebra::{finite_map::KeyProperties, IndexedZSet, MapBuilder, ZRingValue, ZSet},
    circuit::{
        operator_traits::{BinaryOperator, Operator},
        Circuit, Scope, Stream,
    },
    operator::BinaryOperatorAdapter,
    SharedRef,
};
use std::{borrow::Cow, marker::PhantomData};

impl<P, SR1> Stream<Circuit<P>, SR1>
where
    P: Clone + 'static,
{
    /// Apply [`Join`] operator to `self` and `other`.
    ///
    /// See [`Join`] operator for more info.
    pub fn join<K, V1, V2, V, W, F, SR2, Z>(
        &self,
        other: &Stream<Circuit<P>, SR2>,
        f: F,
    ) -> Stream<Circuit<P>, Z>
    where
        K: KeyProperties,
        V1: KeyProperties,
        V2: KeyProperties,
        W: ZRingValue,
        SR1: SharedRef + 'static,
        <SR1 as SharedRef>::Target: IndexedZSet<K, V1, W>,
        for<'a> &'a <SR1 as SharedRef>::Target: IntoIterator<
            Item = (
                &'a K,
                &'a <<SR1 as SharedRef>::Target as IndexedZSet<K, V1, W>>::ZSet,
            ),
        >,
        for<'a> &'a <<SR1 as SharedRef>::Target as IndexedZSet<K, V1, W>>::ZSet:
            IntoIterator<Item = (&'a V1, &'a W)>,
        SR2: SharedRef + 'static,
        <SR2 as SharedRef>::Target: IndexedZSet<K, V2, W>,
        for<'a> &'a <SR2 as SharedRef>::Target: IntoIterator<
            Item = (
                &'a K,
                &'a <<SR2 as SharedRef>::Target as IndexedZSet<K, V2, W>>::ZSet,
            ),
        >,
        for<'a> &'a <<SR2 as SharedRef>::Target as IndexedZSet<K, V2, W>>::ZSet:
            IntoIterator<Item = (&'a V2, &'a W)>,
        F: Fn(&K, &V1, &V2) -> V + 'static,
        V: 'static,
        Z: Clone + MapBuilder<V, W> + 'static,
    {
        self.circuit().add_binary_operator(
            <BinaryOperatorAdapter<Z, _>>::new(Join::new(f)),
            self,
            other,
        )
    }
}

impl<P, I1> Stream<Circuit<P>, I1>
where
    P: Clone + 'static,
{
    /// Incremental join of two streams.
    ///
    /// Given streams `a` and `b` of changes to relations `A` and `B`
    /// respectively, computes a stream of changes to `A <> B` (where `<>`
    /// is the join operator):
    ///
    /// ```text
    /// delta(A <> B) = A <> B - z^-1(A) <> z^-1(B) = a <> z^-1(B) + z^-1(A) <> b + a <> b
    /// ```
    pub fn join_incremental<K, V1, V2, V, W, F, I2, Z>(
        &self,
        other: &Stream<Circuit<P>, I2>,
        join_func: F,
    ) -> Stream<Circuit<P>, Z>
    where
        K: KeyProperties,
        V1: KeyProperties,
        V2: KeyProperties,
        W: ZRingValue,
        I1: IndexedZSet<K, V1, W> + SharedRef<Target = I1>,
        for<'a> &'a I1: IntoIterator<Item = (&'a K, &'a I1::ZSet)>,
        for<'a> &'a I1::ZSet: IntoIterator<Item = (&'a V1, &'a W)>,
        I2: IndexedZSet<K, V2, W> + SharedRef<Target = I2>,
        for<'a> &'a I2: IntoIterator<Item = (&'a K, &'a I2::ZSet)>,
        for<'a> &'a I2::ZSet: IntoIterator<Item = (&'a V2, &'a W)>,
        F: Clone + Fn(&K, &V1, &V2) -> V + 'static,
        V: KeyProperties + 'static,
        Z: ZSet<V, W>,
    {
        self.integrate()
            .delay()
            .join(other, join_func.clone())
            .plus(&self.join(&other.integrate(), join_func))
    }

    /// Incremental join of two nested streams.
    ///
    /// Given nested streams `a` and `b` of changes to relations `A` and `B`,
    /// computes `(↑((↑(a <> b))∆))∆` using the following formula:
    ///
    /// ```text
    /// (↑((↑(a <> b))∆))∆ =
    ///     I(↑I(a)) <> b            +
    ///     ↑I(a) <> I(z^-1(b))      +
    ///     a <> I(↑I(↑z^-1(b)))     +
    ///     I(z^-1(a)) <> ↑I(↑z^-1(b)).
    /// ```
    pub fn join_incremental_nested<K, V1, V2, V, W, F, I2, Z>(
        &self,
        other: &Stream<Circuit<P>, I2>,
        join_func: F,
    ) -> Stream<Circuit<P>, Z>
    where
        K: KeyProperties,
        V1: KeyProperties,
        V2: KeyProperties,
        W: ZRingValue,
        I1: IndexedZSet<K, V1, W> + SharedRef<Target = I1> + std::fmt::Debug,
        for<'a> &'a I1: IntoIterator<Item = (&'a K, &'a I1::ZSet)>,
        for<'a> &'a I1::ZSet: IntoIterator<Item = (&'a V1, &'a W)>,
        I2: IndexedZSet<K, V2, W> + SharedRef<Target = I2> + std::fmt::Debug,
        for<'a> &'a I2: IntoIterator<Item = (&'a K, &'a I2::ZSet)>,
        for<'a> &'a I2::ZSet: IntoIterator<Item = (&'a V2, &'a W)>,
        F: Clone + Fn(&K, &V1, &V2) -> V + 'static,
        V: KeyProperties + 'static,
        Z: ZSet<V, W>,
    {
        let join1: Stream<_, Z> = self
            .integrate()
            .integrate_nested()
            .join(other, join_func.clone());
        let join2 = self
            .integrate()
            .join(&other.integrate_nested().delay_nested(), join_func.clone());
        let join3 = self.join(
            &other.integrate().integrate_nested().delay(),
            join_func.clone(),
        );
        let join4 = self
            .integrate_nested()
            .delay_nested()
            .join(&other.integrate().delay(), join_func);
        join1.sum(&[join2, join3, join4])
    }
}

/// Join two indexed Z-sets.
///
/// The operator takes two streams of indexed Z-sets and outputs
/// a stream obtained by joining each pair of inputs.
///
/// An indexed Z-set is a map from keys to a Z-set of values associated
/// with each key.  Both input streams must use the same key type `K`.
/// Indexed Z-sets are produced for example by the
/// [`Index`](`crate::operator::Index`) operator.
///
/// # Type arguments
///
/// * `K` - key type.
/// * `V1` - value type in the first input stream.
/// * `V2` - value type in the second input stream.
/// * `V` - value type in the output Z-set.
/// * `W` - weight type used by both inputs.
/// * `F` - join function type: maps key and a pair of values from input Z-sets
///   to an output value.
/// * `I1` - indexed Z-set type in the first input stream.
/// * `I1` - indexed Z-set type in the second input stream.
/// * `Z` - output Z-set type.
pub struct Join<K, V1, V2, V, W, F, I1, I2, Z> {
    join_func: F,
    _val_types: PhantomData<(K, V1, V2, V, W)>,
    _container_types: PhantomData<(I1, I2, Z)>,
}

impl<K, V1, V2, V, W, F, I1, I2, Z> Join<K, V1, V2, V, W, F, I1, I2, Z> {
    pub fn new(join_func: F) -> Self {
        Self {
            join_func,
            _val_types: PhantomData,
            _container_types: PhantomData,
        }
    }
}

impl<K, V1, V2, V, W, F, I1, I2, Z> Operator for Join<K, V1, V2, V, W, F, I1, I2, Z>
where
    K: 'static,
    V1: 'static,
    V2: 'static,
    W: 'static,
    I1: 'static,
    I2: 'static,
    F: 'static,
    V: 'static,
    Z: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Join")
    }
    fn clock_start(&mut self, _scope: Scope) {}
    fn clock_end(&mut self, _scope: Scope) {}
}

fn join_inner<K, V1, V2, V, W, F, I1, I2, Z>(i1: &I1, i2: &I2, join_func: &F) -> Z
where
    K: KeyProperties,
    V1: KeyProperties,
    V2: KeyProperties,
    W: ZRingValue,
    I1: IndexedZSet<K, V1, W>,
    for<'a> &'a I1: IntoIterator<Item = (&'a K, &'a I1::ZSet)>,
    for<'a> &'a I1::ZSet: IntoIterator<Item = (&'a V1, &'a W)>,
    I2: IndexedZSet<K, V2, W>,
    for<'a> &'a I2::ZSet: IntoIterator<Item = (&'a V2, &'a W)>,
    F: Fn(&K, &V1, &V2) -> V,
    Z: MapBuilder<V, W> + 'static,
{
    let mut map = Z::empty();
    for (k, vals1) in i1.into_iter() {
        if let Some(vals2) = i2.get_in_support(k) {
            for (v1, w1) in vals1.into_iter() {
                for (v2, w2) in vals2.into_iter() {
                    map.increment_owned(join_func(k, v1, v2), w1.mul_by_ref(w2));
                }
            }
        }
    }
    map
}

impl<K, V1, V2, V, W, F, I1, I2, Z> BinaryOperator<I1, I2, Z>
    for Join<K, V1, V2, V, W, F, I1, I2, Z>
where
    K: KeyProperties,
    V1: KeyProperties,
    V2: KeyProperties,
    W: ZRingValue,
    I1: IndexedZSet<K, V1, W>,
    for<'a> &'a I1: IntoIterator<Item = (&'a K, &'a I1::ZSet)>,
    for<'a> &'a I1::ZSet: IntoIterator<Item = (&'a V1, &'a W)>,
    I2: IndexedZSet<K, V2, W>,
    for<'a> &'a I2: IntoIterator<Item = (&'a K, &'a I2::ZSet)>,
    for<'a> &'a I2::ZSet: IntoIterator<Item = (&'a V2, &'a W)>,
    F: Fn(&K, &V1, &V2) -> V + 'static,
    V: 'static,
    Z: MapBuilder<V, W> + 'static,
{
    fn eval(&mut self, i1: &I1, i2: &I2) -> Z {
        // Iterate over smaller index.
        if i1.support_size() < i2.support_size() {
            join_inner(i1, i2, &self.join_func)
        } else {
            join_inner(i2, i1, &|k, i2, i1| (self.join_func)(k, i1, i2))
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        algebra::{FiniteHashMap, HasZero},
        circuit::{Root, Stream},
        finite_map,
        operator::{DelayedFeedback, Generator},
    };
    use std::vec;

    #[test]
    fn join_test() {
        let root = Root::build(move |circuit| {
            let mut input1 = vec![
                vec![
                    ((1, "a"), 1),
                    ((1, "b"), 2),
                    ((2, "c"), 3),
                    ((2, "d"), 4),
                    ((3, "e"), 5),
                    ((3, "f"), -2),
                ],
                vec![((1, "a"), 1)],
                vec![((1, "a"), 1)],
                vec![((4, "n"), 2)],
                vec![((1, "a"), 0)],
            ]
            .into_iter();
            let mut input2 = vec![
                vec![
                    ((2, "g"), 3),
                    ((2, "h"), 4),
                    ((3, "i"), 5),
                    ((3, "j"), -2),
                    ((4, "k"), 5),
                    ((4, "l"), -2),
                ],
                vec![((1, "b"), 1)],
                vec![((4, "m"), 1)],
                vec![],
                vec![],
            ]
            .into_iter();
            let mut outputs = vec![
                finite_map! {
                    (2, "c g".to_string()) => 9,
                    (2, "c h".to_string()) => 12,
                    (2, "d g".to_string()) => 12,
                    (2, "d h".to_string()) => 16,
                    (3, "e i".to_string()) => 25,
                    (3, "e j".to_string()) => -10,
                    (3, "f i".to_string()) => -10,
                    (3, "f j".to_string()) => 4
                },
                finite_map! {
                    (1, "a b".to_string()) => 1,
                },
                finite_map! {},
                finite_map! {},
                finite_map! {},
            ]
            .into_iter();
            let mut inc_outputs = vec![
                finite_map! {
                    (2, "c g".to_string()) => 9,
                    (2, "c h".to_string()) => 12,
                    (2, "d g".to_string()) => 12,
                    (2, "d h".to_string()) => 16,
                    (3, "e i".to_string()) => 25,
                    (3, "e j".to_string()) => -10,
                    (3, "f i".to_string()) => -10,
                    (3, "f j".to_string()) => 4
                },
                finite_map! {
                    (1, "a b".to_string()) => 2,
                    (1, "b b".to_string()) => 2,
                },
                finite_map! {
                    (1, "a b".to_string()) => 1,
                },
                finite_map! {
                    (4, "n k".to_string()) => 10,
                    (4, "n l".to_string()) => -4,
                    (4, "n m".to_string()) => 2,
                },
                finite_map! {},
            ]
            .into_iter();

            let index1: Stream<_, FiniteHashMap<usize, FiniteHashMap<&'static str, isize>>> =
                circuit
                    .add_source(Generator::new(move || input1.next().unwrap()))
                    .index();
            let index2: Stream<_, FiniteHashMap<usize, FiniteHashMap<&'static str, isize>>> =
                circuit
                    .add_source(Generator::new(move || input2.next().unwrap()))
                    .index();
            index1
                .join(&index2, |k: &usize, s1, s2| {
                    (k.clone(), format!("{} {}", s1, s2))
                })
                .inspect(move |fm: &FiniteHashMap<(usize, String), _>| {
                    assert_eq!(fm, &outputs.next().unwrap())
                });
            index1
                .join_incremental(&index2, |k: &usize, s1, s2| {
                    (k.clone(), format!("{} {}", s1, s2))
                })
                .inspect(move |fm: &FiniteHashMap<(usize, String), _>| {
                    assert_eq!(fm, &inc_outputs.next().unwrap())
                });
        })
        .unwrap();

        for _ in 0..5 {
            root.step().unwrap();
        }
    }

    // Nested incremental reachability algorithm.
    #[test]
    fn join_incremental_nested_test() {
        let root = Root::build(move |circuit| {
            // Changes to the edges relation.
            let mut edges: vec::IntoIter<FiniteHashMap<(usize, usize), isize>> = vec![
                finite_map! { (1, 2) => 1 },
                finite_map! { (2, 3) => 1},
                finite_map! { (1, 3) => 1},
                finite_map! { (3, 1) => 1},
                finite_map! { (3, 1) => -1},
                finite_map! { (1, 2) => -1},
                finite_map! { (2, 4) => 1, (4, 1) => 1 },
                finite_map! { (2, 3) => -1, (3, 2) => 1 },
            ]
            .into_iter();

            // Expected content of the reachability relation.
            let mut outputs: vec::IntoIter<FiniteHashMap<(usize, usize), isize>> = vec![
                finite_map! { (1, 2) => 1 },
                finite_map! { (1, 2) => 1, (2, 3) => 1, (1, 3) => 1 },
                finite_map! { (1, 2) => 1, (2, 3) => 1, (1, 3) => 1 },
                finite_map! { (1, 1) => 1, (2, 2) => 1, (3, 3) => 1, (1, 2) => 1, (1, 3) => 1, (2, 3) => 1, (2, 1) => 1, (3, 1) => 1, (3, 2) => 1},
                finite_map! { (1, 2) => 1, (2, 3) => 1, (1, 3) => 1 },
                finite_map! { (2, 3) => 1, (1, 3) => 1 },
                finite_map! { (1, 3) => 1, (2, 3) => 1, (2, 4) => 1, (2, 1) => 1, (4, 1) => 1, (4, 3) => 1 },
                finite_map! { (1, 1) => 1, (2, 2) => 1, (3, 3) => 1, (4, 4) => 1,
                              (1, 2) => 1, (1, 3) => 1, (1, 4) => 1,
                              (2, 1) => 1, (2, 3) => 1, (2, 4) => 1,
                              (3, 1) => 1, (3, 2) => 1, (3, 4) => 1,
                              (4, 1) => 1, (4, 2) => 1, (4, 3) => 1 },
            ]
            .into_iter();

            let edges: Stream<_, FiniteHashMap<(usize, usize), isize>> =
                circuit
                    .add_source(Generator::new(move || edges.next().unwrap()));

            let paths = circuit.iterate_with_conditions(|child| {
                // ```text
                //                      distinct_incremental_nested
                //               ┌───┐          ┌───┐
                // edges         │   │          │   │  paths
                // ────┬────────►│ + ├──────────┤   ├────────┬───►
                //     │         │   │          │   │        │
                //     │         └───┘          └───┘        │
                //     │           ▲                         │
                //     │           │                         │
                //     │         ┌─┴─┐                       │
                //     │         │   │                       │
                //     └────────►│ X │ ◄─────────────────────┘
                //               │   │
                //               └───┘
                //      join_incremental_nested
                // ```
                let edges = edges.delta0(child);
                let paths_delayed = DelayedFeedback::new(child);

                let paths_inverted: Stream<_, FiniteHashMap<(usize, usize), isize>> = paths_delayed
                    .stream()
                    .apply(|paths: &FiniteHashMap<(usize, usize), isize>| paths.into_iter().map(|((x,y), w)| ((*y, *x), *w)).collect());

                let paths_inverted_indexed: Stream<_, FiniteHashMap<usize, FiniteHashMap<usize, isize>>> = paths_inverted.index();
                let edges_indexed: Stream<_, FiniteHashMap<usize, FiniteHashMap<usize, isize>>> = edges.index();

                let paths = edges.plus(&paths_inverted_indexed.join_incremental_nested(&edges_indexed, |_via, from, to| (*from, *to)))
                    .distinct_incremental_nested();
                paths_delayed.connect(&paths);
                let output = paths.integrate();
                Ok((
                    vec![
                        paths.condition(HasZero::is_zero),
                        paths.integrate_nested().condition(HasZero::is_zero)
                    ],
                    output.export(),
                ))
            })
            .unwrap();

            paths.integrate().distinct().inspect(move |ps| {
                assert_eq!(*ps, outputs.next().unwrap());
            })
        })
        .unwrap();

        for _ in 0..8 {
            root.step().unwrap();
        }
    }
}
