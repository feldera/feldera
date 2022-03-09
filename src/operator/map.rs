//! Map operators.

use crate::{
    circuit::{
        operator_traits::{Operator, UnaryOperator},
        Circuit, Scope, Stream,
    },
    RefPair,
};
use std::{borrow::Cow, marker::PhantomData};

impl<P, CI> Stream<Circuit<P>, CI>
where
    CI: Clone,
    P: Clone + 'static,
{
    /// Apply [`MapKeys`] operator to `self`.
    ///
    /// The `map` closure takes input keys by reference.
    pub fn map_keys<K1, K2, V, CO, F>(&self, map: F) -> Stream<Circuit<P>, CO>
    where
        K1: Clone + 'static,
        K2: Clone + 'static,
        V: Clone + 'static,
        CI: IntoIterator<Item = (K1, V)> + 'static,
        for<'a> &'a CI: IntoIterator,
        for<'a> <&'a CI as IntoIterator>::Item: RefPair<'a, K1, V>,
        CO: FromIterator<(K2, V)> + Clone + 'static,
        F: Fn(&K1) -> K2 + Clone + 'static,
    {
        self.circuit()
            .add_unary_operator(MapKeys::new(map.clone(), move |x| (map)(&x)), self)
    }

    /// Apply [`MapKeys`] operator to `self`.
    ///
    /// The `map` closure operates on owned keys.
    pub fn map_keys_owned<K1, K2, V, CO, F>(&self, map: F) -> Stream<Circuit<P>, CO>
    where
        K1: Clone + 'static,
        K2: Clone + 'static,
        V: Clone + 'static,
        CI: IntoIterator<Item = (K1, V)> + 'static,
        for<'a> &'a CI: IntoIterator,
        for<'a> <&'a CI as IntoIterator>::Item: RefPair<'a, K1, V>,
        CO: FromIterator<(K2, V)> + Clone + 'static,
        F: Fn(K1) -> K2 + Clone + 'static,
    {
        let func_clone = map.clone();
        self.circuit().add_unary_operator(
            MapKeys::new(move |x: &K1| (map)(x.clone()), func_clone),
            self,
        )
    }
}

/// Operator that applies a user-defined function to each value in a collection
/// of key/value pairs.
///
/// # Type arguments
///
/// * `K1` - input key type.
/// * `K2` - output key type.
/// * `V` - value type.
/// * `CI` - input collection type.
/// * `CO` - output collection type.
/// * `FB` - key mapping function type that takes a borrowed key.
/// * `FO` - key mapping function type that takes an owned key.
pub struct MapKeys<K1, K2, V, CI, CO, FB, FO>
where
    FB: 'static,
    FO: 'static,
{
    map_borrowed: FB,
    map_owned: FO,
    _type: PhantomData<(K1, K2, V, CI, CO)>,
}

impl<K1, K2, V, CI, CO, FB, FO> MapKeys<K1, K2, V, CI, CO, FB, FO>
where
    FB: 'static,
    FO: 'static,
{
    pub fn new(map_borrowed: FB, map_owned: FO) -> Self {
        Self {
            map_borrowed,
            map_owned,
            _type: PhantomData,
        }
    }
}

impl<K1, K2, V, CI, CO, FB, FO> Operator for MapKeys<K1, K2, V, CI, CO, FB, FO>
where
    K1: 'static,
    K2: 'static,
    V: 'static,
    CI: 'static,
    CO: 'static,
    FB: 'static,
    FO: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("MapKeys")
    }
    fn clock_start(&mut self, _scope: Scope) {}
    fn clock_end(&mut self, _scope: Scope) {}
}

impl<K1, K2, V, CI, CO, FB, FO> UnaryOperator<CI, CO> for MapKeys<K1, K2, V, CI, CO, FB, FO>
where
    K1: Clone + 'static,
    K2: Clone + 'static,
    V: Clone + 'static,
    CI: IntoIterator<Item = (K1, V)> + 'static,
    for<'a> &'a CI: IntoIterator,
    for<'a> <&'a CI as IntoIterator>::Item: RefPair<'a, K1, V>,
    CO: FromIterator<(K2, V)> + 'static,
    FB: Fn(&K1) -> K2 + 'static,
    FO: Fn(K1) -> K2 + 'static,
{
    fn eval(&mut self, i: &CI) -> CO {
        i.into_iter()
            .map(|pair| {
                let (k, v) = pair.into_refs();
                ((self.map_borrowed)(k), v.clone())
            })
            .collect()
    }

    fn eval_owned(&mut self, i: CI) -> CO {
        i.into_iter()
            .map(|(k, v)| ((self.map_owned)(k), v))
            .collect()
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
    use std::vec;

    #[test]
    fn map_keys_test() {
        let root = Root::build(move |circuit| {
            let mut input_map: vec::IntoIter<FiniteHashMap<isize, isize>> =
                vec![finite_map! { 1 => 1, -1 => 1, 5 => 1 }].into_iter();
            let mut times2_output: vec::IntoIter<FiniteHashMap<isize, isize>> =
                vec![finite_map! { 2 => 1, -2 => 1, 10 => 1 }].into_iter();
            let mut times2_pos_output: vec::IntoIter<FiniteHashMap<isize, isize>> =
                vec![finite_map! { 2 => 1, 10 => 1 }].into_iter();
            let mut neg_output: vec::IntoIter<FiniteHashMap<isize, isize>> =
                vec![finite_map! { -1 => 1, 1 => 1, -5 => 1}].into_iter();
            let mut neg_pos_output: vec::IntoIter<FiniteHashMap<isize, isize>> =
                vec![finite_map! { -1 => 1, -5 => 1}].into_iter();

            let mut input_vec: vec::IntoIter<Vec<(isize, isize)>> =
                vec![vec![(1, 1), (-1, 1)]].into_iter();
            let mut abs_output: vec::IntoIter<FiniteHashMap<isize, isize>> =
                vec![finite_map! { 1 => 2 }].into_iter();
            let mut abs_pos_output: vec::IntoIter<FiniteHashMap<isize, isize>> =
                vec![finite_map! { 1 => 1 }].into_iter();
            let mut sqr_output: vec::IntoIter<FiniteHashMap<isize, isize>> =
                vec![finite_map! { 1 => 2, }].into_iter();
            let mut sqr_pos_output: vec::IntoIter<FiniteHashMap<isize, isize>> =
                vec![finite_map! { 1 => 1, }].into_iter();

            let input_map_stream =
                circuit.add_source(Generator::new(move || input_map.next().unwrap()));
            let input_vec_stream =
                circuit.add_source(Generator::new(move || input_vec.next().unwrap()));
            let times2: Stream<_, FiniteHashMap<_, _>> = input_map_stream.map_keys(|n| n * 2);
            let times2_pos: Stream<_, FiniteHashMap<_, _>> =
                input_map_stream.filter_map_keys(|n| if *n > 0 { Some(n * 2) } else { None });
            let neg: Stream<_, FiniteHashMap<_, _>> = input_map_stream.map_keys_owned(|n| -n);
            let neg_pos: Stream<_, FiniteHashMap<_, _>> =
                input_map_stream.filter_map_keys_owned(|n| if n > 0 { Some(-n) } else { None });
            let abs: Stream<_, FiniteHashMap<_, _>> = input_vec_stream.map_keys(|n| n.abs());
            let abs_pos: Stream<_, FiniteHashMap<_, _>> =
                input_vec_stream.filter_map_keys(|n| if *n > 0 { Some(n.abs()) } else { None });
            let sqr: Stream<_, FiniteHashMap<_, _>> = input_vec_stream.map_keys_owned(|n| n * n);
            let sqr_pos: Stream<_, FiniteHashMap<_, _>> =
                input_vec_stream.filter_map_keys_owned(|n| if n > 0 { Some(n * n) } else { None });
            times2.inspect(move |n| {
                assert_eq!(*n, times2_output.next().unwrap());
            });
            times2_pos.inspect(move |n| {
                assert_eq!(*n, times2_pos_output.next().unwrap());
            });
            neg.inspect(move |n| {
                assert_eq!(*n, neg_output.next().unwrap());
            });
            neg_pos.inspect(move |n| {
                assert_eq!(*n, neg_pos_output.next().unwrap());
            });
            abs.inspect(move |n| {
                assert_eq!(*n, abs_output.next().unwrap());
            });
            abs_pos.inspect(move |n| {
                assert_eq!(*n, abs_pos_output.next().unwrap());
            });
            sqr.inspect(move |n| {
                assert_eq!(*n, sqr_output.next().unwrap());
            });
            sqr_pos.inspect(move |n| {
                assert_eq!(*n, sqr_pos_output.next().unwrap());
            });
        })
        .unwrap();

        for _ in 0..1 {
            root.step().unwrap();
        }
    }
}
