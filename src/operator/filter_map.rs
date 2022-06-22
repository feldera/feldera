//! Filter-map operators.

use crate::{
    circuit::{
        operator_traits::{Operator, UnaryOperator},
        Circuit, Scope, Stream,
    },
    trace::{Batch, BatchReader, Cursor},
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
    pub fn filter_map_keys<CO, F>(&self, func: F) -> Stream<Circuit<P>, CO>
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

    /// Apply [`FilterMapKeys`] operator to `self`.
    ///
    /// The `func` closure operates on owned keys.
    pub fn filter_map_keys_owned<CO, F>(&self, func: F) -> Stream<Circuit<P>, CO>
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
