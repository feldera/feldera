//! Defines an operator whose input is a stream of z-sets and which applies
//! a z-set filtering operation to each input pointwise.

use crate::{
    algebra::{finite_map::KeyProperties, zset::ZSet, ZRingValue},
    circuit::{
        operator_traits::{Operator, UnaryOperator},
        Circuit, Stream,
    },
};
use std::{borrow::Cow, marker::PhantomData};

impl<P, Z> Stream<Circuit<P>, Z>
where
    P: Clone + 'static,
{
    pub fn filter_keys<F, T, W>(&self, func: F) -> Stream<Circuit<P>, Z>
    where
        F: Fn(&T) -> bool + 'static,
        T: KeyProperties,
        W: ZRingValue,
        Z: ZSet<T, W>,
    {
        self.circuit()
            .add_unary_operator(ZSetFilter::new(func), self)
    }
}

/// A ZSetFilter operator consumes ZSets and produces ZSets.
/// Each output ZSet is produced by applying a filtering
/// function to the input ZSet.  A filtering function
/// produces a boolean for each tuple.
/// `T` type of tuples stored in ZSet
/// `W` type of weights of ZSet
/// `F` type of function that filters a Zset
/// `Z` a ZSet<T, W>
pub struct ZSetFilter<T, W, F, Z>
where
    T: KeyProperties,
    W: ZRingValue,
    Z: ZSet<T, W>,
    F: 'static,
{
    filter: F,
    _type: PhantomData<(T, W, Z)>,
}

impl<T, W, F, Z> ZSetFilter<T, W, F, Z>
where
    T: KeyProperties,
    W: ZRingValue,
    Z: ZSet<T, W>,
    F: 'static,
{
    /// Create a new ZSetFilter operator.
    /// `filter`: a function that consumes a tuple and produces a boolean.
    #[allow(dead_code)]
    pub fn new(filter: F) -> Self {
        ZSetFilter {
            filter,
            _type: PhantomData,
        }
    }
}

impl<T, W, F, Z> Operator for ZSetFilter<T, W, F, Z>
where
    T: KeyProperties,
    W: ZRingValue,
    Z: ZSet<T, W>,
    F: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("ZSetFilter")
    }
    fn clock_start(&mut self) {}
    fn clock_end(&mut self) {}
}

impl<T, W, F, Z> UnaryOperator<Z, Z> for ZSetFilter<T, W, F, Z>
where
    T: KeyProperties,
    W: ZRingValue,
    Z: ZSet<T, W>,
    F: Fn(&T) -> bool + 'static,
{
    fn eval(&mut self, i: &Z) -> Z {
        i.filter(&self.filter)
    }

    fn eval_owned(&mut self, i: Z) -> Z {
        i.filter(&self.filter)
    }
}
