//! Map operators.

use crate::{
    circuit::{
        operator_traits::{Operator, UnaryOperator},
        Circuit, Scope, Stream,
    },
    trace::{cursor::Cursor, Batch, BatchReader},
};
use std::{borrow::Cow, marker::PhantomData};

impl<P, B> Stream<Circuit<P>, B>
where
    B: Clone,
    P: Clone + 'static,
{
    /// Apply [`MapKeys`] operator to `self`.
    ///
    /// The `map` closure takes input keys by reference.
    pub fn map_keys<CO, F>(&self, map: F) -> Stream<Circuit<P>, CO>
    where
        B: BatchReader<Time = ()> + 'static,
        CO: Batch<Val = B::Val, Time = (), R = B::R> + Clone + 'static,
        CO::Val: Clone,
        F: Fn(&B::Key) -> CO::Key + Clone + 'static,
    {
        self.circuit()
            .add_unary_operator(MapKeys::new(map.clone(), move |x| (map)(&x)), self)
    }

    /// Apply [`MapKeys`] operator to `self`.
    ///
    /// The `map` closure operates on owned keys.
    pub fn map_keys_owned<CO, F>(&self, map: F) -> Stream<Circuit<P>, CO>
    where
        B: BatchReader<Time = ()> + 'static,
        B::Key: Clone,
        CO: Batch<Val = B::Val, Time = (), R = B::R> + Clone + 'static,
        CO::Val: Clone,
        F: Fn(B::Key) -> CO::Key + Clone + 'static,
    {
        let func_clone = map.clone();
        self.circuit().add_unary_operator(
            MapKeys::new(move |x: &B::Key| (map)(x.clone()), func_clone),
            self,
        )
    }

    /// Apply [`MapValues`] operator to `self`.
    pub fn map_values<CO, F>(&self, map: F) -> Stream<Circuit<P>, CO>
    where
        B: BatchReader<Time = (), Key = CO::Key, R = CO::R> + 'static,
        CO: Batch<Time = ()> + Clone + 'static,
        CO::Key: Clone,
        F: Fn(&B::Key, &B::Val) -> CO::Val + 'static,
    {
        self.circuit().add_unary_operator(MapValues::new(map), self)
    }
}

/// Operator that applies a user-defined function to each value in a collection
/// of key/value pairs.
///
/// # Type arguments
///
/// * `CI` - input collection type.
/// * `CO` - output collection type.
/// * `FB` - key mapping function type that takes a borrowed key.
/// * `FO` - key mapping function type that takes an owned key.
pub struct MapKeys<CI, CO, FB, FO>
where
    FB: 'static,
    FO: 'static,
{
    map_borrowed: FB,
    _map_owned: FO,
    _type: PhantomData<(CI, CO)>,
}

impl<CI, CO, FB, FO> MapKeys<CI, CO, FB, FO>
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

impl<CI, CO, FB, FO> Operator for MapKeys<CI, CO, FB, FO>
where
    CI: 'static,
    CO: 'static,
    FB: 'static,
    FO: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("MapKeys")
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<CI, CO, FB, FO> UnaryOperator<CI, CO> for MapKeys<CI, CO, FB, FO>
where
    CI: BatchReader<Time = ()> + 'static,
    CI::Val: Clone,
    CO: Batch<Val = CI::Val, Time = (), R = CI::R> + 'static,
    FB: Fn(&CI::Key) -> CO::Key + 'static,
    FO: Fn(CI::Key) -> CO::Key + 'static,
{
    fn eval(&mut self, i: &CI) -> CO {
        let mut batch = Vec::with_capacity(i.len());

        let mut cursor = i.cursor();
        while cursor.key_valid() {
            while cursor.val_valid() {
                let w = cursor.weight();
                let v = cursor.val();
                let k = cursor.key();
                batch.push((((self.map_borrowed)(k), v.clone()), w.clone()));
                cursor.step_val();
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

/// Operator that applies a user-defined function to each value in a collection
/// of key/value pairs.
///
/// # Type arguments
///
/// * `CI` - input collection type.
/// * `CO` - output collection type.
/// * `F` - function that maps input key-value pairs into output values.
pub struct MapValues<CI, CO, F>
where
    F: 'static,
{
    map: F,
    _type: PhantomData<(CI, CO)>,
}

impl<CI, CO, F> MapValues<CI, CO, F>
where
    F: 'static,
{
    pub fn new(map: F) -> Self {
        Self {
            map,
            _type: PhantomData,
        }
    }
}

impl<CI, CO, F> Operator for MapValues<CI, CO, F>
where
    CI: 'static,
    CO: 'static,
    F: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("MapValues")
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<CI, CO, F> UnaryOperator<CI, CO> for MapValues<CI, CO, F>
where
    CI: BatchReader<Time = ()> + 'static,
    CI::Key: Clone,
    CO: Batch<Key = CI::Key, Time = (), R = CI::R> + 'static,
    F: Fn(&CI::Key, &CI::Val) -> CO::Val + 'static,
{
    fn eval(&mut self, i: &CI) -> CO {
        let mut batch = Vec::with_capacity(i.len());

        let mut cursor = i.cursor();
        while cursor.key_valid() {
            let k = cursor.key().clone();
            while cursor.val_valid() {
                let w = cursor.weight().clone();
                let v = cursor.val();
                batch.push(((k.clone(), (self.map)(&k, v)), w));
                cursor.step_val();
            }
            cursor.step_key();
        }
        CO::from_tuples((), batch)
    }

    fn eval_owned(&mut self, i: CI) -> CO {
        self.eval(&i)
    }
}

#[cfg(test)]
mod test {
    use crate::{
        circuit::{Root, Stream},
        operator::Generator,
        trace::ord::OrdZSet,
        zset,
    };
    use std::vec;

    #[test]
    fn map_keys_test() {
        let root = Root::build(move |circuit| {
            let mut input_map: vec::IntoIter<OrdZSet<isize, isize>> =
                vec![zset! { 1 => 1, -1 => 1, 5 => 1 }].into_iter();
            let mut times2_output: vec::IntoIter<OrdZSet<isize, isize>> =
                vec![zset! { 2 => 1, -2 => 1, 10 => 1 }].into_iter();
            let mut times2_pos_output: vec::IntoIter<OrdZSet<isize, isize>> =
                vec![zset! { 2 => 1, 10 => 1 }].into_iter();
            let mut neg_output: vec::IntoIter<OrdZSet<isize, isize>> =
                vec![zset! { -1 => 1, 1 => 1, -5 => 1}].into_iter();
            let mut neg_pos_output: vec::IntoIter<OrdZSet<isize, isize>> =
                vec![zset! { -1 => 1, -5 => 1}].into_iter();

            let mut input_vec: vec::IntoIter<OrdZSet<isize, isize>> =
                vec![zset! { 1 => 1, -1 => 1}].into_iter();
            let mut abs_output: vec::IntoIter<OrdZSet<isize, isize>> =
                vec![zset! { 1 => 2 }].into_iter();
            let mut abs_pos_output: vec::IntoIter<OrdZSet<isize, isize>> =
                vec![zset! { 1 => 1 }].into_iter();
            let mut sqr_output: vec::IntoIter<OrdZSet<isize, isize>> =
                vec![zset! { 1 => 2, }].into_iter();
            let mut sqr_pos_output: vec::IntoIter<OrdZSet<isize, isize>> =
                vec![zset! { 1 => 1, }].into_iter();

            let input_map_stream =
                circuit.add_source(Generator::new(move || input_map.next().unwrap()));
            let input_vec_stream =
                circuit.add_source(Generator::new(move || input_vec.next().unwrap()));
            let times2: Stream<_, OrdZSet<_, _>> = input_map_stream.map_keys(|n| n * 2);
            let times2_pos: Stream<_, OrdZSet<_, _>> =
                input_map_stream.filter_map_keys(|n| if *n > 0 { Some(n * 2) } else { None });
            let neg: Stream<_, OrdZSet<_, _>> = input_map_stream.map_keys_owned(|n| -n);
            let neg_pos: Stream<_, OrdZSet<_, _>> =
                input_map_stream.filter_map_keys_owned(|n| if n > 0 { Some(-n) } else { None });
            let abs: Stream<_, OrdZSet<_, _>> = input_vec_stream.map_keys(|n| n.abs());
            let abs_pos: Stream<_, OrdZSet<_, _>> =
                input_vec_stream.filter_map_keys(|n| if *n > 0 { Some(n.abs()) } else { None });
            let sqr: Stream<_, OrdZSet<_, _>> = input_vec_stream.map_keys_owned(|n| n * n);
            let sqr_pos: Stream<_, OrdZSet<_, _>> =
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
