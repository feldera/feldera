//! Filter-map operators.

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
    /// Apply [`FilterMapKeys`] operator to `self`.
    ///
    /// The `func` closure takes input keys by reference.
    pub fn filter_map_keys<K1, K2, V, CO, F>(&self, func: F) -> Stream<Circuit<P>, CO>
    where
        K1: Clone + 'static,
        K2: Clone + 'static,
        V: Clone + 'static,
        CI: IntoIterator<Item = (K1, V)> + 'static,
        for<'a> &'a CI: IntoIterator,
        for<'a> <&'a CI as IntoIterator>::Item: RefPair<'a, K1, V>,
        CO: FromIterator<(K2, V)> + Clone + 'static,
        F: Fn(&K1) -> Option<K2> + Clone + 'static,
    {
        self.circuit()
            .add_unary_operator(FilterMapKeys::new(func.clone(), move |x| (func)(&x)), self)
    }

    /// Apply [`FilterMapKeys`] operator to `self`.
    ///
    /// The `func` closure operates on owned keys.
    pub fn filter_map_keys_owned<K1, K2, V, CO, F>(&self, func: F) -> Stream<Circuit<P>, CO>
    where
        K1: Clone + 'static,
        K2: Clone + 'static,
        V: Clone + 'static,
        CI: IntoIterator<Item = (K1, V)> + 'static,
        for<'a> &'a CI: IntoIterator,
        for<'a> <&'a CI as IntoIterator>::Item: RefPair<'a, K1, V>,
        CO: FromIterator<(K2, V)> + Clone + 'static,
        F: Fn(K1) -> Option<K2> + Clone + 'static,
    {
        let func_clone = func.clone();
        self.circuit().add_unary_operator(
            FilterMapKeys::new(move |x: &K1| (func)(x.clone()), func_clone),
            self,
        )
    }
}

/// Operator that both filters and maps keys in a collection of key/value
/// pairs.
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
pub struct FilterMapKeys<K1, K2, V, CI, CO, FB, FO>
where
    FB: 'static,
    FO: 'static,
{
    map_borrowed: FB,
    map_owned: FO,
    _type: PhantomData<(K1, K2, V, CI, CO)>,
}

impl<K1, K2, V, CI, CO, FB, FO> FilterMapKeys<K1, K2, V, CI, CO, FB, FO>
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

impl<K1, K2, V, CI, CO, FB, FO> Operator for FilterMapKeys<K1, K2, V, CI, CO, FB, FO>
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
        Cow::from("FilterMapKeys")
    }
    fn clock_start(&mut self, _scope: Scope) {}
    fn clock_end(&mut self, _scope: Scope) {}
}

impl<K1, K2, V, CI, CO, FB, FO> UnaryOperator<CI, CO> for FilterMapKeys<K1, K2, V, CI, CO, FB, FO>
where
    K1: Clone + 'static,
    K2: Clone + 'static,
    V: Clone + 'static,
    CI: IntoIterator<Item = (K1, V)> + 'static,
    for<'a> &'a CI: IntoIterator,
    for<'a> <&'a CI as IntoIterator>::Item: RefPair<'a, K1, V>,
    CO: FromIterator<(K2, V)> + 'static,
    FB: Fn(&K1) -> Option<K2> + 'static,
    FO: Fn(K1) -> Option<K2> + 'static,
{
    fn eval(&mut self, i: &CI) -> CO {
        i.into_iter()
            .filter_map(|pair| {
                let (k, v) = pair.into_refs();
                (self.map_borrowed)(k).map(|k| (k, v.clone()))
            })
            .collect()
    }

    fn eval_owned(&mut self, i: CI) -> CO {
        i.into_iter()
            .filter_map(|(k, v)| (self.map_owned)(k).map(|k| (k, v)))
            .collect()
    }
}
