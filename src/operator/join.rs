use crate::{
    algebra::{finite_map::KeyProperties, IndexedZSet, MapBuilder, ZRingValue},
    circuit::{
        operator_traits::{BinaryOperator, Operator},
        Circuit, Scope, Stream,
    },
};
use std::{borrow::Cow, marker::PhantomData};

impl<P, I1> Stream<Circuit<P>, I1>
where
    P: Clone + 'static,
{
    /// Apply [`Join`] operator to `self` and `other`.
    ///
    /// See [`Join`] operator for more info.
    pub fn join<K, V1, V2, V, W, F, I2, Z>(
        &self,
        other: &Stream<Circuit<P>, I2>,
        f: F,
    ) -> Option<Stream<Circuit<P>, Z>>
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
        Z: Clone + MapBuilder<V, W> + 'static,
    {
        self.circuit()
            .add_binary_operator(Join::new(f), self, other)
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
        algebra::FiniteHashMap,
        circuit::{Root, Stream},
        finite_map,
        operator::Generator,
    };

    #[test]
    fn join_test() {
        let root = Root::build(move |circuit| {
            let mut input1 = vec![vec![
                ((1, "a"), 1),
                ((1, "b"), 2),
                ((2, "c"), 3),
                ((2, "d"), 4),
                ((3, "e"), 5),
                ((3, "f"), -2),
            ]]
            .into_iter();
            let mut input2 = vec![vec![
                ((2, "g"), 3),
                ((2, "h"), 4),
                ((3, "i"), 5),
                ((3, "j"), -2),
                ((4, "k"), 5),
                ((4, "l"), -2),
            ]]
            .into_iter();
            let mut outputs = vec![finite_map! {
                (2, "c g".to_string()) => 9,
                (2, "c h".to_string()) => 12,
                (2, "d g".to_string()) => 12,
                (2, "d h".to_string()) => 16,
                (3, "e i".to_string()) => 25,
                (3, "e j".to_string()) => -10,
                (3, "f i".to_string()) => -10,
                (3, "f j".to_string()) => 4
            }]
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
                .unwrap()
                .inspect(move |fm: &FiniteHashMap<(usize, String), _>| {
                    assert_eq!(fm, &outputs.next().unwrap())
                });
        })
        .unwrap();

        for _ in 0..1 {
            root.step().unwrap();
        }
    }
}
