//! Filtering operators.

use crate::{
    circuit::{
        operator_traits::{Operator, UnaryOperator},
        Circuit, Stream,
    },
};
use std::{borrow::Cow, marker::PhantomData};

impl<P, CI> Stream<Circuit<P>, CI>
where
    CI: Clone,
    P: Clone + 'static,
{
    /// Apply [`FilterKeys`] operator to `self`.
    pub fn filter_keys<K, V, CO, F>(&self, func: F) -> Stream<Circuit<P>, CO>
    where
        K: Clone + 'static,
        V: Clone + 'static,
        CI: IntoIterator<Item=(K, V)> + 'static,
        for <'a> &'a CI: IntoIterator<Item=(&'a K, &'a V)>,
        CO: FromIterator<(K, V)> + Clone + 'static,
        F: Fn(&K) -> bool + 'static,
    {
        self.circuit()
            .add_unary_operator(FilterKeys::new(func), self)
    }
}

/// Operator that filters a collection of key/value pairs based on keys.
///
/// The operator applies a filtering function to each key in the input
/// collection and builds an output collection containing only the elements
/// that satisfy the filter condition.
///
/// # Type arguments
///
/// * `K` - key type.
/// * `V` - value type.
/// * `CI` - input collection type.
/// * `CO` - output collection type.
/// * `F` - filtering function type.
pub struct FilterKeys<K, V, CI, CO, F>
where
    F: 'static,
{
    filter: F,
    _type: PhantomData<(K, V, CI, CO)>,
}

impl<K, V, CI, CO, F> FilterKeys<K, V, CI, CO, F>
where
    F: 'static,
{
    pub fn new(filter: F) -> Self {
        Self {
            filter,
            _type: PhantomData,
        }
    }
}

impl<K, V, CI, CO, F> Operator for FilterKeys<K, V, CI, CO, F>
where
    K: 'static,
    V: 'static,
    CI: 'static,
    CO: 'static,
    F: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("FilterKeys")
    }
    fn clock_start(&mut self) {}
    fn clock_end(&mut self) {}
}

impl<K, V, CI, CO, F> UnaryOperator<CI, CO> for FilterKeys<K, V, CI, CO, F>
where
    K: Clone + 'static,
    V: Clone + 'static,
    CI: IntoIterator<Item=(K, V)> + 'static,
    for <'a> &'a CI: IntoIterator<Item=(&'a K, &'a V)>,
    CO: FromIterator<(K, V)> + 'static,
    F: Fn(&K) -> bool + 'static,
{
    fn eval(&mut self, i: &CI) -> CO {
        i.into_iter().filter_map(|(k, v)| if (self.filter)(k) {Some((k.clone(), v.clone()))} else {None}).collect()
    }

    fn eval_owned(&mut self, i: CI) -> CO {
        i.into_iter().filter(|(k, _v)| (self.filter)(&k)).collect()
    }
}
