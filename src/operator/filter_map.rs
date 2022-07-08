//! Filter-map operators.

// TODO: This can be easily generalized to flat_map by generalizing `func` to
// return any `Iterator` type rather than `Some`.

use crate::{
    circuit::{
        operator_traits::{Operator, UnaryOperator},
        Circuit, Scope, Stream,
    },
    trace::{Batch, BatchReader, Cursor},
    OrdIndexedZSet, OrdZSet,
};
use std::{borrow::Cow, marker::PhantomData};

impl<P, CI> Stream<Circuit<P>, CI>
where
    CI: Clone,
    P: Clone + 'static,
{
    /// Both filter and map input batches with a user-provided closure.
    ///
    /// The operator applies `func` to each key in the input
    /// batch and builds an output batch containing only the keys
    /// returned by `func` as `Some(key)`.
    ///
    /// This method returns output batches of type [`OrdZSet`].
    /// Use [`Self::filter_map_keys_generic`] to create other types
    /// of output batches.
    pub fn filter_map<F, T>(&self, func: F) -> Stream<Circuit<P>, OrdZSet<T, CI::R>>
    where
        CI: BatchReader<Val = (), Time = ()> + 'static,
        T: Ord + Clone + 'static,
        F: Fn(&CI::Key) -> Option<T> + Clone + 'static,
    {
        self.filter_map_keys_generic(func)
    }

    /// Both filter and map keys in the input batch with a user-provided
    /// closure.
    ///
    /// For each `(key, value)` pair in the input batch, the operator computes
    /// `func(key)`. If the result is `None`, the pair is dropped.  If the
    /// result is `Some(newkey)`, the `(newkey, value)` pair is added to the
    /// output batch.
    ///
    /// This method returns output batches of type [`OrdIndexedZSet`].
    /// Use [`Self::filter_map_keys_generic`] to create other types of output
    /// batches.
    pub fn filter_map_keys<F, T>(
        &self,
        func: F,
    ) -> Stream<Circuit<P>, OrdIndexedZSet<T, CI::Val, CI::R>>
    where
        CI: BatchReader<Time = ()> + 'static,
        CI::Val: Clone + Ord,
        T: Ord + Clone + 'static,
        F: Fn(&CI::Key) -> Option<T> + Clone + 'static,
    {
        self.filter_map_keys_generic(func)
    }

    /// Both filter and map keys in the input batch with a user-provided
    /// closure.
    ///
    /// For each `(key, value)` pair in the input batch, the operator computes
    /// `func(key)`. If the result is `None`, the pair is dropped.  If the
    /// result is `Some(newkey)`, the `(newkey, value)` pair is added to the
    /// output batch.
    ///
    /// This method is generic over output batch type.
    /// Use `Self::filter_map` to return a batch of type [`OrdZSet`].
    /// Use `Self::filter_map_keys` to return a batch of type
    /// [`OrdIndexedZSet`].
    pub fn filter_map_keys_generic<CO, F>(&self, func: F) -> Stream<Circuit<P>, CO>
    where
        CI: BatchReader<Time = ()> + 'static,
        CI::Val: Clone,
        CO: Batch<Val = CI::Val, Time = (), R = CI::R> + Clone + 'static,
        CO::Key: Clone,
        F: Fn(&CI::Key) -> Option<CO::Key> + Clone + 'static,
    {
        self.circuit()
            .add_unary_operator(FilterMapKeys::new(func.clone(), move |x| (func)(&x)), self)
    }

    /// Like [`Self::filter_map`], but takes a closure that consumes inputs by
    /// value.
    pub fn filter_map_owned<F, T>(&self, func: F) -> Stream<Circuit<P>, OrdZSet<T, CI::R>>
    where
        CI: BatchReader<Val = (), Time = ()> + 'static,
        CI::Key: Clone,
        T: Ord + Clone + 'static,
        F: Fn(CI::Key) -> Option<T> + Clone + 'static,
    {
        self.filter_map_keys_owned_generic(func)
    }

    /// Like [`Self::filter_map_keys`], but takes a closure that consumes inputs
    /// by value.
    pub fn filter_map_keys_owned<F, T>(
        &self,
        func: F,
    ) -> Stream<Circuit<P>, OrdIndexedZSet<T, CI::Val, CI::R>>
    where
        CI: BatchReader<Time = ()> + 'static,
        CI::Key: Clone,
        CI::Val: Clone + Ord,
        T: Ord + Clone + 'static,
        F: Fn(CI::Key) -> Option<T> + Clone + 'static,
    {
        self.filter_map_keys_owned_generic(func)
    }

    /// Like [`Self::filter_map_keys_generic`], but takes a closure that
    /// consumes inputs by value.
    pub fn filter_map_keys_owned_generic<CO, F>(&self, func: F) -> Stream<Circuit<P>, CO>
    where
        CI: BatchReader<Time = ()> + 'static,
        CI::Key: Clone,
        CI::Val: Clone,
        CO: Batch<Val = CI::Val, Time = (), R = CI::R> + Clone + 'static,
        CO::Key: Clone,
        F: Fn(CI::Key) -> Option<CO::Key> + Clone + 'static,
    {
        let func_clone = func.clone();
        self.circuit().add_unary_operator(
            FilterMapKeys::new(move |x: &CI::Key| (func)(x.clone()), func_clone),
            self,
        )
    }
}

/// Operator that both filters and maps keys in a batch.
///
/// # Type arguments
///
/// * `CI` - input batch type.
/// * `CO` - output batch type.
/// * `FB` - key mapping function type that takes a borrowed key.
/// * `FO` - key mapping function type that takes an owned key.
pub struct FilterMapKeys<CI, CO, FB, FO>
where
    FB: 'static,
    FO: 'static,
{
    map_borrowed: FB,
    _map_owned: FO,
    _type: PhantomData<(CI, CO)>,
}

impl<CI, CO, FB, FO> FilterMapKeys<CI, CO, FB, FO>
where
    FB: 'static,
    FO: 'static,
{
    pub fn new(map_borrowed: FB, _map_owned: FO) -> Self {
        Self {
            map_borrowed,
            _map_owned,
            _type: PhantomData,
        }
    }
}

impl<CI, CO, FB, FO> Operator for FilterMapKeys<CI, CO, FB, FO>
where
    CI: 'static,
    CO: 'static,
    FB: 'static,
    FO: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("FilterMapKeys")
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<CI, CO, FB, FO> UnaryOperator<CI, CO> for FilterMapKeys<CI, CO, FB, FO>
where
    CI: BatchReader<Time = ()> + 'static,
    CI::Val: Clone,
    CO: Batch<Val = CI::Val, Time = (), R = CI::R> + 'static,
    CO::Key: Clone,
    FB: Fn(&CI::Key) -> Option<CO::Key> + 'static,
    FO: Fn(CI::Key) -> Option<CO::Key> + 'static,
{
    fn eval(&mut self, i: &CI) -> CO {
        let mut cursor = i.cursor();
        let mut batch = Vec::with_capacity(i.len());
        while cursor.key_valid() {
            let k = cursor.key();
            if let Some(k2) = (self.map_borrowed)(k) {
                while cursor.val_valid() {
                    let w = cursor.weight();
                    let v = cursor.val();
                    batch.push(((k2.clone(), v.clone()), w.clone()));
                    cursor.step_val();
                }
            }
            cursor.step_key();
        }

        CO::from_tuples((), batch)
    }

    fn eval_owned(&mut self, i: CI) -> CO {
        // TODO: owned implementation.
        self.eval(&i)
    }
}

#[cfg(test)]
mod test {
    use crate::{circuit::Root, indexed_zset, operator::Generator, zset, OrdIndexedZSet, OrdZSet};
    use std::vec;

    #[test]
    fn filter_map_test() {
        let root = Root::build(move |circuit| {
            let mut inputs: vec::IntoIter<OrdZSet<isize, isize>> = vec![
                zset! { 1 => 1, 2 => 1, 3 => -1 },
                zset! { -1 => 1, -2 => 1, -3 => -1 },
                zset! { -1 => 1, 2 => 1, 3 => -1 },
            ]
            .into_iter();
            let expected_outputs_vec = vec![
                zset! { -1 => 1, -2 => 1, -3 => -1 },
                zset! {},
                zset! { -2 => 1, -3 => -1 },
            ];

            let mut expected_outputs1 = expected_outputs_vec.clone().into_iter();
            let mut expected_outputs2 = expected_outputs_vec.into_iter();

            let source = circuit.add_source(Generator::new(move || inputs.next().unwrap()));

            let output_stream1 = source.filter_map(|x| if *x > 0 { Some(-x) } else { None });

            let output_stream2 = source.filter_map_owned(|x| if x > 0 { Some(-x) } else { None });

            output_stream1.inspect(move |zs| {
                assert_eq!(*zs, expected_outputs1.next().unwrap());
            });
            output_stream2.inspect(move |zs| {
                assert_eq!(*zs, expected_outputs2.next().unwrap());
            });
        })
        .unwrap();

        for _ in 0..3 {
            root.step().unwrap();
        }
    }

    #[test]
    fn filter_map_keys_test() {
        let root = Root::build(move |circuit| {
            let mut inputs: vec::IntoIter<OrdIndexedZSet<isize, &'static str, isize>> = vec![
                indexed_zset! { 1 => {"1" => 1}, 2 => {"2" => 1}, 3 => {"3" => -1} },
                indexed_zset! { -1 => {"-1" => 1}, -2 => {"-2" => 1}, -3 => {"-3" => -1} },
                indexed_zset! { -1 => {"-1" => 1}, 2 => {"2" => 1}, 3 => {"3" => -1} },
            ]
            .into_iter();
            let expected_outputs_vec = vec![
                indexed_zset! { -1 => {"1" => 1}, -2 => {"2"=> 1}, -3 => {"3"=> -1} },
                indexed_zset! {},
                indexed_zset! { -2 => {"2" => 1}, -3 => {"3" => -1} },
            ];

            let mut expected_outputs1 = expected_outputs_vec.clone().into_iter();
            let mut expected_outputs2 = expected_outputs_vec.into_iter();

            let source = circuit.add_source(Generator::new(move || inputs.next().unwrap()));

            let output_stream1 = source.filter_map_keys(|x| if *x > 0 { Some(-x) } else { None });

            let output_stream2 =
                source.filter_map_keys_owned(|x| if x > 0 { Some(-x) } else { None });

            output_stream1.inspect(move |zs| {
                assert_eq!(*zs, expected_outputs1.next().unwrap());
            });
            output_stream2.inspect(move |zs| {
                assert_eq!(*zs, expected_outputs2.next().unwrap());
            });
        })
        .unwrap();

        for _ in 0..3 {
            root.step().unwrap();
        }
    }
}
