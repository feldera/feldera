use core::ops::{Add};
use num::PrimInt;

// Macro to create variants of an aggregation function
// There must exist a function f__(left: T, right: T) -> S.
// This creates 3 more functions
// f_N_(left: Option<T>, right: T) -> Option<S>
// etc.
// And 4 more functions:
// f_conditional__(left: T, right: T, predicate: bool) -> S
#[macro_export]
macro_rules! some_agg_function {
    ($func_name:ident, $bounds: ty) => {
        ::paste::paste! {
            pub fn [<$func_name _N_>]<T>( left: Option<T>, right: T ) -> Option<T>
                where T: $bounds
            {
                let left = left?;
                Some([<$func_name __>](left, right))
            }

            pub fn [<$func_name __N>]<T>( left: T, right: Option<T> ) -> Option<T>
                where T: $bounds
            {
                let right = right?;
                Some([<$func_name __>](left, right))
            }

            pub fn [<$func_name _N_N>]<T>( left: Option<T>, right: Option<T> ) -> Option<T>
                where T: $bounds
            {
                let left = left?;
                let right = right?;
                Some([<$func_name __>](left, right))
            }

            pub fn [<$func_name _conditional__>]<T>( left: T, right: T, predicate: bool ) -> T
                where T: $bounds
            {
                if predicate {
                    [<$func_name __>](left, right)
                } else {
                    left
                }
            }

            pub fn [<$func_name _conditional_N_>]<T>( left: Option<T>, right: T, predicate: bool ) -> Option<T>
                where T: $bounds
            {
                match (left, right, predicate) {
                    (_, _, false) => left,
                    (None, _, _) => Some(right),
                    (Some(x), _, _) => Some([<$func_name __>](x, right)),
                }
            }

            pub fn [<$func_name _conditional__N>]<T>( left: T, right: Option<T>, predicate: bool ) -> Option<T>
                where T: $bounds
            {
                match (left, right, predicate) {
                    (_, _, false) => Some(left),
                    (_, None, _) => Some(left),
                    (_, Some(y), _) => Some([<$func_name __>](left, y)),
                }
            }

            pub fn [<$func_name _conditional_N_N>]<T>( left: Option<T>, right: Option<T>, predicate: bool ) -> Option<T>
                where T: $bounds
            {
                match (left, right, predicate) {
                    (_, _, false) => left,
                    (None, _, _) => right,
                    (Some(_), None, _) => left,
                    (Some(x), Some(y), _) => Some([<$func_name __>](x, y)),
                }
            }
        }
    };
}

pub fn agg_max__<T>(left: T, right: T) -> T
where
    T: Ord + Copy,
{
    left.max(right)
}

pub fn agg_min__<T>(left: T, right: T) -> T
where
    T: Ord + Copy,
{
    left.min(right)
}

some_agg_function!(agg_max, Ord + Copy);
some_agg_function!(agg_min, Ord + Copy);

pub fn agg_plus__<T>(left: T, right: T) -> T
where
    T: Add<T, Output = T> + Copy,
{
    agg_plus_conditional__(left, right, true)
}

some_agg_function!(agg_plus, Add<T, Output = T> + Copy);

#[inline(always)]
fn agg_and__<T>(left: T, right: T) -> T
where
    T: PrimInt
{
    left & right
}

#[inline(always)]
fn agg_or__<T>(left: T, right: T) -> T
where
    T: PrimInt
{
    left | right
}

#[inline(always)]
fn agg_xor__<T>(left: T, right: T) -> T
where
    T: PrimInt
{
    left ^ right
}

some_agg_function!(agg_and, PrimInt);
some_agg_function!(agg_or, PrimInt);
some_agg_function!(agg_xor, PrimInt);

