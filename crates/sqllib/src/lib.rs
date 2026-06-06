#![allow(non_snake_case)]

#[doc(hidden)]
pub mod aggregates;
pub use aggregates::*;
pub mod array;
pub use array::*;
#[doc(hidden)]
pub mod binary;
pub use binary::*;
#[doc(hidden)]
pub mod boolean;
pub use boolean::*;
#[doc(hidden)]
pub mod casts;
pub use casts::*;
#[doc(hidden)]
pub mod decimal;
pub use decimal::*;
#[doc(hidden)]
pub mod error;
pub use error::*;
#[doc(hidden)]
pub mod float;
pub use float::*;
#[doc(hidden)]
pub mod geopoint;
pub use geopoint::*;
#[doc(hidden)]
pub mod interval;
pub use interval::*;
#[doc(hidden)]
pub mod map;
pub use map::*;
#[doc(hidden)]
pub mod operators;
pub use operators::*;
#[doc(hidden)]
pub mod source;
pub use source::*;
#[doc(hidden)]
pub mod string;
pub use string::*;
#[doc(hidden)]
pub mod timestamp;
pub use timestamp::*;
#[doc(hidden)]
pub mod uuid;
pub use uuid::*;
#[doc(hidden)]
pub mod variant;
pub use variant::*;
#[doc(hidden)]
pub mod rfc3339;
#[doc(hidden)]
pub use num_traits::Float;
pub use regex::Regex;
#[doc(hidden)]
pub use source::{SourcePosition, SourcePositionRange};

mod string_interner;
pub use string_interner::{build_string_interner, intern_string, unintern_string};

// Re-export these types, so they can be used in UDFs without having to import the dbsp crate directly.
// Perhaps they should be defined in sqllib in the first place?
pub use dbsp::algebra::{F32, F64};
use dbsp::{
    DBData, MapHandle, OrdIndexedZSet, OrdZSet, OutputHandle, ZSetHandle, ZWeight,
    algebra::{
        AddByRef, HasOne, HasZero, NegByRef, OrdIndexedZSetFactories, OrdZSetFactories, Semigroup,
        SemigroupValue, ZRingValue,
    },
    circuit::metrics::TOTAL_LATE_RECORDS,
    dynamic::{DowncastTrait, DynData, Erase},
    operator::Update,
    trace::{
        BatchReader, BatchReaderFactories, Builder, Cursor,
        ord::{OrdIndexedWSetBuilder, OrdWSetBuilder},
    },
    typed_batch::{SpineSnapshot, TypedBatch},
    utils::*,
};
use num::{PrimInt, Signed};
use num_traits::Pow;
use std::marker::PhantomData;
use std::ops::{Deref, Neg};
use std::sync::OnceLock;
use std::{fmt::Debug, sync::atomic::Ordering};

#[allow(dead_code)]
#[doc(hidden)]
pub(crate) fn div_round_nearest<T>(a: T, b: T) -> T
where
    T: PrimInt + Signed,
{
    // Code tries to avoid overflows
    debug_assert!(b > T::zero());
    let q = a / b;
    let r = (a % b).abs();
    if r.is_zero() {
        return q;
    }

    if r > b - r {
        q + a.signum()
    } else if r < b - r {
        q
    } else {
        // Tie
        let two = T::one() + T::one();
        if (q % two).is_zero() {
            q
        } else {
            q + a.signum()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------
    // divisor = 1000
    // -----------------------------
    #[test]
    fn test_rounding_1000() {
        assert_eq!(div_round_nearest(0, 1000), 0);
        assert_eq!(div_round_nearest(1, 1000), 0);
        assert_eq!(div_round_nearest(499, 1000), 0);
        assert_eq!(div_round_nearest(500, 1000), 0);
        assert_eq!(div_round_nearest(501, 1000), 1);
        assert_eq!(div_round_nearest(999, 1000), 1);
        assert_eq!(div_round_nearest(1000, 1000), 1);
        assert_eq!(div_round_nearest(1499, 1000), 1);
        assert_eq!(div_round_nearest(1500, 1000), 2);
        assert_eq!(div_round_nearest(1501, 1000), 2);

        assert_eq!(div_round_nearest(-1, 1000), 0);
        assert_eq!(div_round_nearest(-499, 1000), 0);
        assert_eq!(div_round_nearest(-500, 1000), 0);
        assert_eq!(div_round_nearest(-501, 1000), -1);
        assert_eq!(div_round_nearest(-999, 1000), -1);
        assert_eq!(div_round_nearest(-1000, 1000), -1);
        assert_eq!(div_round_nearest(-1499, 1000), -1);
        assert_eq!(div_round_nearest(-1500, 1000), -2);
        assert_eq!(div_round_nearest(-1501, 1000), -2);
    }

    #[test]
    fn test_extremes() {
        assert_eq!(div_round_nearest(i64::MAX, 1000), 9_223_372_036_854_776);
        assert_eq!(div_round_nearest(i64::MIN, 1000), -9_223_372_036_854_776);
    }

    #[test]
    fn test_rounding_7() {
        assert_eq!(div_round_nearest(0, 7), 0);
        assert_eq!(div_round_nearest(1, 7), 0);
        assert_eq!(div_round_nearest(2, 7), 0);
        assert_eq!(div_round_nearest(3, 7), 0);
        assert_eq!(div_round_nearest(4, 7), 1);
        assert_eq!(div_round_nearest(5, 7), 1);
        assert_eq!(div_round_nearest(6, 7), 1);
        assert_eq!(div_round_nearest(7, 7), 1);
        assert_eq!(div_round_nearest(8, 7), 1);
        assert_eq!(div_round_nearest(9, 7), 1);
        assert_eq!(div_round_nearest(10, 7), 1);
        assert_eq!(div_round_nearest(11, 7), 2);

        assert_eq!(div_round_nearest(-1, 7), 0);
        assert_eq!(div_round_nearest(-2, 7), 0);
        assert_eq!(div_round_nearest(-3, 7), 0);
        assert_eq!(div_round_nearest(-4, 7), -1);
        assert_eq!(div_round_nearest(-5, 7), -1);
        assert_eq!(div_round_nearest(-6, 7), -1);
        assert_eq!(div_round_nearest(-7, 7), -1);
        assert_eq!(div_round_nearest(-8, 7), -1);
        assert_eq!(div_round_nearest(-9, 7), -1);
        assert_eq!(div_round_nearest(-10, 7), -1);
        assert_eq!(div_round_nearest(-11, 7), -2);
    }
}

/// Convert a value of a SQL data type to an integer
/// that preserves ordering.  Used for partitioned_rolling_aggregates
#[doc(hidden)]
pub trait ToInteger<T>
where
    T: PrimInt,
{
    #[doc(hidden)]
    fn to_integer(&self) -> T;
}

/// Trait that provides the inverse functionality of the ToInteger trait.
/// Used for partitioned_rolling_aggregates
#[doc(hidden)]
pub trait FromInteger<T>
where
    T: PrimInt,
{
    #[doc(hidden)]
    fn from_integer(value: &T) -> Self;
}

#[doc(hidden)]
impl<T> ToInteger<T> for T
where
    T: PrimInt,
{
    #[doc(hidden)]
    fn to_integer(&self) -> T {
        *self
    }
}

#[doc(hidden)]
impl<T> FromInteger<T> for T
where
    T: PrimInt,
{
    #[doc(hidden)]
    fn from_integer(value: &T) -> Self {
        *value
    }
}

#[doc(hidden)]
pub type Weight = ZWeight;
#[doc(hidden)]
pub type WSet<D> = OrdZSet<D>;
#[doc(hidden)]
pub type IndexedWSet<K, D> = OrdIndexedZSet<K, D>;

/// Internal base used by [`some_function1`]–[`some_function4`].
///
/// Given a base function `f` with *N* non-optional arguments and return type
/// `R`, generates all 2^N − 1 nullable-wrapper variants: one for each
/// non-empty subset of arguments that may be `Option<T>`.  The
/// all-non-nullable combination is skipped because that is the base function.
///
/// Each argument type is passed wrapped in parentheses (e.g. `(i32)`) so that
/// generic types like `Array<i32>` survive token-tree accumulation as a single
/// `tt`; Rust treats `(T)` as a type identical to `T` in all positions.
///
/// State carried through `@step`:
/// - `[$($tgen:tt)*]`          — type-generic declarations, e.g. `T: Clone, U: Eq`; empty when absent
/// - `[$(($dd, $dt, $dn)),*]` — decided args: `(suffix char, paren-wrapped type, arg ident)`
/// - `[$($base)*]`             — base suffix: one `_` per arg, used to name the base function
/// - `$wrap`                   — `y` to wrap the base call in `Some(…)`, `n` to return directly
/// - `$has_n`                  — `y` once any nullable branch is taken, `n` otherwise
/// - `[$(($an, $at)),*]`       — remaining args: `(arg ident, paren-wrapped type)`
#[doc(hidden)]
#[macro_export]
macro_rules! some_function_impl {
    // ── helpers ────────────────────────────────────────────────────────────
    // Produce the parameter type: `_` → bare type, `N` → Option-wrapped type.
    // The argument is always a paren-wrapped type `($t)`; the pattern `($t:ty)`
    // strips the outer parens so the emitted type is `T`, not `(T)`.
    (@type _ ($t:ty)) => { $t };
    (@type N ($t:ty)) => { Option<$t> };

    // Produce the unwrap statement: `_` → nothing, `N` → `let x = x?;`
    (@unwrap _ $n:ident) => {};
    (@unwrap N $n:ident) => { let $n = $n?; };

    // ── combination generator ───────────────────────────────────────────────
    // Recursive step: branch into non-nullable (`_`) and nullable (`N`).
    // `$tgen` is threaded through unchanged.
    (@step $fn:ident, $ret:ty, [$($tgen:tt)*],
     [$(($dd:tt, $dt:tt, $dn:ident)),*], [$($base:tt)*]; $wrap:tt; $has_n:tt;
     [($an:ident, $at:tt) $(, ($rn:ident, $rt:tt))*]
    ) => {
        $crate::some_function_impl!(@step $fn, $ret, [$($tgen)*],
            [$(($dd, $dt, $dn),)* (_, $at, $an)], [$($base)* _]; $wrap; $has_n;
            [$(($rn, $rt)),*]
        );
        $crate::some_function_impl!(@step $fn, $ret, [$($tgen)*],
            [$(($dd, $dt, $dn),)* (N, $at, $an)], [$($base)* _]; $wrap; y;
            [$(($rn, $rt)),*]
        );
    };

    // Base: all non-nullable — skip (would duplicate the base function itself).
    (@step $fn:ident, $ret:ty, [$($tgen:tt)*],
     [$(($dd:tt, $dt:tt, $dn:ident)),*], [$($base:tt)*]; $wrap:tt; n; []
    ) => {};

    // Base: ≥1 nullable, no type generics, wrap in Some().
    (@step $fn:ident, $ret:ty, [],
     [$(($dd:tt, $dt:tt, $dn:ident)),*], [$($base:tt)*]; y; y; []
    ) => {
        ::paste::paste! {
            #[doc(hidden)]
            pub fn [<$fn $($dd)*>](
                $( $dn: $crate::some_function_impl!(@type $dd $dt) ),*
            ) -> Option<$ret> {
                $( $crate::some_function_impl!(@unwrap $dd $dn); )*
                Some([<$fn $($base)*>]($($dn),*))
            }
        }
    };

    // Base: ≥1 nullable, type generics present, wrap in Some().
    (@step $fn:ident, $ret:ty, [$($tgen:tt)+],
     [$(($dd:tt, $dt:tt, $dn:ident)),*], [$($base:tt)*]; y; y; []
    ) => {
        ::paste::paste! {
            #[doc(hidden)]
            pub fn [<$fn $($dd)*>]< $($tgen)* >(
                $( $dn: $crate::some_function_impl!(@type $dd $dt) ),*
            ) -> Option<$ret> {
                $( $crate::some_function_impl!(@unwrap $dd $dn); )*
                Some([<$fn $($base)*>]($($dn),*))
            }
        }
    };

    // Base: ≥1 nullable, no type generics, direct return.
    (@step $fn:ident, $ret:ty, [],
     [$(($dd:tt, $dt:tt, $dn:ident)),*], [$($base:tt)*]; n; y; []
    ) => {
        ::paste::paste! {
            #[doc(hidden)]
            pub fn [<$fn $($dd)*>](
                $( $dn: $crate::some_function_impl!(@type $dd $dt) ),*
            ) -> Option<$ret> {
                $( $crate::some_function_impl!(@unwrap $dd $dn); )*
                [<$fn $($base)*>]($($dn),*)
            }
        }
    };

    // Base: ≥1 nullable, type generics present, direct return.
    (@step $fn:ident, $ret:ty, [$($tgen:tt)+],
     [$(($dd:tt, $dt:tt, $dn:ident)),*], [$($base:tt)*]; n; y; []
    ) => {
        ::paste::paste! {
            #[doc(hidden)]
            pub fn [<$fn $($dd)*>]< $($tgen)* >(
                $( $dn: $crate::some_function_impl!(@type $dd $dt) ),*
            ) -> Option<$ret> {
                $( $crate::some_function_impl!(@unwrap $dd $dn); )*
                [<$fn $($base)*>]($($dn),*)
            }
        }
    };
}

/// Macro to create variants of a function with 1 argument.
/// If there exists a function `f_(x: T) -> S`, this creates:
/// - `fN(x: Option<T>) -> Option<S>`
///
/// The generated function returns `None` when any argument is `None`.
#[doc(hidden)]
#[macro_export]
macro_rules! some_function1 {
    ($func_name:ident, $arg_type:ty, $ret_type:ty) => {
        $crate::some_function_impl!(@step $func_name, $ret_type, [], [], []; y; n;
            [(arg0, ($arg_type))]
        );
    };
    ($func_name:ident [$($tgen:tt)*], $arg_type:ty, $ret_type:ty) => {
        $crate::some_function_impl!(@step $func_name, $ret_type, [$($tgen)*], [], []; y; n;
            [(arg0, ($arg_type))]
        );
    };
}

/// Internal base used by [`some_polymorphic_function1`]–[`some_polymorphic_function3`]
/// and [`some_polymorphic_null_function3`].
///
/// Generates all 2^N − 1 nullable-wrapper variants of a polymorphic function whose
/// name includes SQL type names (e.g. `f_t0N_t1_t2`).
///
/// State carried through `@step`:
/// - `[$($cdecl:tt)*]`, `[$($cinvoke:tt)*]` — const-generic declaration and invocation
///   tokens (e.g. `const P: usize, const S: usize` and `P, S`), or `[]`/`[]` when absent.
/// - `$wrap` — `y` to wrap the base call in `Some(…)`, `n` to return it directly.
/// - `[$(($dd, $dt, $dn)),*]` — decided args: `(suffix char, paren type, arg ident)`.
/// - `[$($sfx)*]`, `[$($bsfx)*]` — variant name suffix and base name suffix token lists.
/// - `$has_n` — `y` once any nullable branch is taken.
/// - `[$(($an, $atn, $at)),*]` — remaining args: `(arg ident, type name ident, paren type)`.
#[doc(hidden)]
#[macro_export]
macro_rules! some_polymorphic_function_impl {
    // ── Recursive step ─────────────────────────────────────────────────────
    // Branch into non-nullable (`_`) and nullable (`N`) for the next argument.
    // The type name is embedded directly into the accumulated suffix tokens.
    (@step $fn:ident, $ret:ty,
     [$($cdecl:tt)*], [$($cinvoke:tt)*]; $wrap:tt;
     [$(($dd:tt, $dt:tt, $dn:ident)),*], [$($sfx:tt)*], [$($bsfx:tt)*]; $has_n:tt;
     [($an:ident, $atn:ident, $at:tt) $(, ($rn:ident, $rtn:ident, $rt:tt))*]
    ) => {
        $crate::some_polymorphic_function_impl!(@step $fn, $ret,
            [$($cdecl)*], [$($cinvoke)*]; $wrap;
            [$(($dd, $dt, $dn),)* (_, $at, $an)], [$($sfx)* _ $atn], [$($bsfx)* _ $atn]; $has_n;
            [$(($rn, $rtn, $rt)),*]
        );
        $crate::some_polymorphic_function_impl!(@step $fn, $ret,
            [$($cdecl)*], [$($cinvoke)*]; $wrap;
            [$(($dd, $dt, $dn),)* (N, $at, $an)], [$($sfx)* _ $atn N], [$($bsfx)* _ $atn]; y;
            [$(($rn, $rtn, $rt)),*]
        );
    };

    // ── Base: all non-nullable — skip ──────────────────────────────────────
    (@step $fn:ident, $ret:ty,
     [$($cdecl:tt)*], [$($cinvoke:tt)*]; $wrap:tt;
     [$(($dd:tt, $dt:tt, $dn:ident)),*], [$($sfx:tt)*], [$($bsfx:tt)*]; n; []
    ) => {};

    // ── Base: ≥1 nullable, no const generics, wrap in Some() ──────────────
    (@step $fn:ident, $ret:ty,
     [], []; y;
     [$(($dd:tt, $dt:tt, $dn:ident)),*], [$($sfx:tt)*], [$($bsfx:tt)*]; y; []
    ) => {
        ::paste::paste! {
            #[doc(hidden)]
            pub fn [<$fn $($sfx)*>](
                $( $dn: $crate::some_function_impl!(@type $dd $dt) ),*
            ) -> Option<$ret> {
                $( $crate::some_function_impl!(@unwrap $dd $dn); )*
                Some([<$fn $($bsfx)*>]($($dn),*))
            }
        }
    };

    // ── Base: ≥1 nullable, const generics present, wrap in Some() ─────────
    (@step $fn:ident, $ret:ty,
     [$($cdecl:tt)+], [$($cinvoke:tt)+]; y;
     [$(($dd:tt, $dt:tt, $dn:ident)),*], [$($sfx:tt)*], [$($bsfx:tt)*]; y; []
    ) => {
        ::paste::paste! {
            #[doc(hidden)]
            pub fn [<$fn $($sfx)*>]< $($cdecl)* >(
                $( $dn: $crate::some_function_impl!(@type $dd $dt) ),*
            ) -> Option<$ret> {
                $( $crate::some_function_impl!(@unwrap $dd $dn); )*
                Some([<$fn $($bsfx)*>]::< $($cinvoke)* >($($dn),*))
            }
        }
    };

    // ── Base: ≥1 nullable, no const generics, direct return ───────────────
    (@step $fn:ident, $ret:ty,
     [], []; n;
     [$(($dd:tt, $dt:tt, $dn:ident)),*], [$($sfx:tt)*], [$($bsfx:tt)*]; y; []
    ) => {
        ::paste::paste! {
            #[doc(hidden)]
            pub fn [<$fn $($sfx)*>](
                $( $dn: $crate::some_function_impl!(@type $dd $dt) ),*
            ) -> Option<$ret> {
                $( $crate::some_function_impl!(@unwrap $dd $dn); )*
                [<$fn $($bsfx)*>]($($dn),*)
            }
        }
    };

    // ── Base: ≥1 nullable, const generics present, direct return ──────────
    (@step $fn:ident, $ret:ty,
     [$($cdecl:tt)+], [$($cinvoke:tt)+]; n;
     [$(($dd:tt, $dt:tt, $dn:ident)),*], [$($sfx:tt)*], [$($bsfx:tt)*]; y; []
    ) => {
        ::paste::paste! {
            #[doc(hidden)]
            pub fn [<$fn $($sfx)*>]< $($cdecl)* >(
                $( $dn: $crate::some_function_impl!(@type $dd $dt) ),*
            ) -> Option<$ret> {
                $( $crate::some_function_impl!(@unwrap $dd $dn); )*
                [<$fn $($bsfx)*>]::< $($cinvoke)* >($($dn),*)
            }
        }
    };
}

/// If there exists `f_t(x: T) -> S`, creates `f_tN(x: Option<T>) -> Option<S>`.
#[doc(hidden)]
#[macro_export]
macro_rules! some_polymorphic_function1 {
    ($func_name:ident, $type_name:ident, $arg_type:ty, $ret_type:ty) => {
        $crate::some_polymorphic_function_impl!(@step $func_name, $ret_type,
            [], []; y;
            [], [], []; n;
            [(arg0, $type_name, ($arg_type))]
        );
    };
    ($func_name:ident [ $(const $var:ident : $vty:ty),* ], $type_name:ident, $arg_type:ty, $ret_type:ty) => {
        $crate::some_polymorphic_function_impl!(@step $func_name, $ret_type,
            [$(const $var: $vty),*], [$($var),*]; y;
            [], [], []; n;
            [(arg0, $type_name, ($arg_type))]
        );
    };
}

/// Macro to create variants of a function with 2 arguments.
/// If there exists a function `f__(x: T, y: S) -> U`, this creates:
/// - `f_N(x: T,         y: Option<S>) -> Option<U>`
/// - `fN_(x: Option<T>, y: S        ) -> Option<U>`
/// - `fNN(x: Option<T>, y: Option<S>) -> Option<U>`
///
/// Each generated function returns `None` when any argument is `None`.
#[doc(hidden)]
#[macro_export]
macro_rules! some_function2 {
    ($func_name:ident, $arg_type0:ty, $arg_type1:ty, $ret_type:ty) => {
        $crate::some_function_impl!(@step $func_name, $ret_type, [], [], []; y; n;
            [(arg0, ($arg_type0)), (arg1, ($arg_type1))]
        );
    };
    ($func_name:ident [$($tgen:tt)*], $arg_type0:ty, $arg_type1:ty, $ret_type:ty) => {
        $crate::some_function_impl!(@step $func_name, $ret_type, [$($tgen)*], [], []; y; n;
            [(arg0, ($arg_type0)), (arg1, ($arg_type1))]
        );
    };
}

/// Like [`some_polymorphic_function1`], but the base function already returns
/// `Option<R>`, so wrappers return it directly without an extra `Some()`.
#[doc(hidden)]
#[macro_export]
macro_rules! some_polymorphic_null_function1 {
    ($func_name:ident, $type_name:ident, $arg_type:ty, $ret_type:ty) => {
        $crate::some_polymorphic_function_impl!(@step $func_name, $ret_type,
            [], []; n;
            [], [], []; n;
            [(arg0, $type_name, ($arg_type))]
        );
    };
    ($func_name:ident [ $(const $var:ident : $vty:ty),* ], $type_name:ident, $arg_type:ty, $ret_type:ty) => {
        $crate::some_polymorphic_function_impl!(@step $func_name, $ret_type,
            [$(const $var: $vty),*], [$($var),*]; n;
            [], [], []; n;
            [(arg0, $type_name, ($arg_type))]
        );
    };
}

/// If there exists `f_t0_t1(x: T, y: S) -> U`, creates:
/// `f_t0_t1N`, `f_t0N_t1`, `f_t0N_t1N`.
#[doc(hidden)]
#[macro_export]
macro_rules! some_polymorphic_function2 {
    ($func_name:ident, $type_name0:ident, $arg_type0:ty, $type_name1:ident, $arg_type1:ty, $ret_type:ty) => {
        $crate::some_polymorphic_function_impl!(@step $func_name, $ret_type,
            [], []; y;
            [], [], []; n;
            [(arg0, $type_name0, ($arg_type0)), (arg1, $type_name1, ($arg_type1))]
        );
    };
    ($func_name:ident [ $(const $var:ident : $vty:ty),* ], $type_name0:ident, $arg_type0:ty, $type_name1:ident, $arg_type1:ty, $ret_type:ty) => {
        $crate::some_polymorphic_function_impl!(@step $func_name, $ret_type,
            [$(const $var: $vty),*], [$($var),*]; y;
            [], [], []; n;
            [(arg0, $type_name0, ($arg_type0)), (arg1, $type_name1, ($arg_type1))]
        );
    };
}

/// Like [`some_polymorphic_function2`], but the base function already returns
/// `Option<R>`, so wrappers return it directly without an extra `Some()`.
#[doc(hidden)]
#[macro_export]
macro_rules! some_polymorphic_null_function2 {
    ($func_name:ident, $type_name0:ident, $arg_type0:ty, $type_name1:ident, $arg_type1:ty, $ret_type:ty) => {
        $crate::some_polymorphic_function_impl!(@step $func_name, $ret_type,
            [], []; n;
            [], [], []; n;
            [(arg0, $type_name0, ($arg_type0)), (arg1, $type_name1, ($arg_type1))]
        );
    };
    ($func_name:ident [ $(const $var:ident : $vty:ty),* ], $type_name0:ident, $arg_type0:ty, $type_name1:ident, $arg_type1:ty, $ret_type:ty) => {
        $crate::some_polymorphic_function_impl!(@step $func_name, $ret_type,
            [$(const $var: $vty),*], [$($var),*]; n;
            [], [], []; n;
            [(arg0, $type_name0, ($arg_type0)), (arg1, $type_name1, ($arg_type1))]
        );
    };
}

/// If there exists `f_t0_t1_t2(x: T, y: S, z: V) -> U`, creates the 7 nullable variants.
#[doc(hidden)]
#[macro_export]
macro_rules! some_polymorphic_function3 {
    ($func_name:ident, $type_name0:ident, $arg_type0:ty, $type_name1:ident, $arg_type1:ty, $type_name2:ident, $arg_type2:ty, $ret_type:ty) => {
        $crate::some_polymorphic_function_impl!(@step $func_name, $ret_type,
            [], []; y;
            [], [], []; n;
            [(arg0, $type_name0, ($arg_type0)), (arg1, $type_name1, ($arg_type1)), (arg2, $type_name2, ($arg_type2))]
        );
    };
}

/// Like [`some_polymorphic_function3`], but the base function already returns
/// `Option<R>`, so wrappers return it directly without an extra `Some()`.
#[doc(hidden)]
#[macro_export]
macro_rules! some_polymorphic_null_function3 {
    ($func_name:ident, $type_name0:ident, $arg_type0:ty, $type_name1:ident, $arg_type1:ty, $type_name2:ident, $arg_type2:ty, $ret_type:ty) => {
        $crate::some_polymorphic_function_impl!(@step $func_name, $ret_type,
            [], []; n;
            [], [], []; n;
            [(arg0, $type_name0, ($arg_type0)), (arg1, $type_name1, ($arg_type1)), (arg2, $type_name2, ($arg_type2))]
        );
    };
    ($func_name:ident [ $(const $var:ident : $vty:ty),* ], $type_name0:ident, $arg_type0:ty, $type_name1:ident, $arg_type1:ty, $type_name2:ident, $arg_type2:ty, $ret_type:ty) => {
        $crate::some_polymorphic_function_impl!(@step $func_name, $ret_type,
            [$(const $var: $vty),*], [$($var),*]; n;
            [], [], []; n;
            [(arg0, $type_name0, ($arg_type0)), (arg1, $type_name1, ($arg_type1)), (arg2, $type_name2, ($arg_type2))]
        );
    };
}

/// Macro to create variants of a function with 3 arguments.
/// If there exists a function `f___(x: T, y: S, z: V) -> U`, this creates
/// the 7 nullable variants (all combinations except all-non-nullable).
/// Each generated function returns `None` when any argument is `None`.
#[doc(hidden)]
#[macro_export]
macro_rules! some_function3 {
    ($func_name:ident, $arg_type0:ty, $arg_type1:ty, $arg_type2:ty, $ret_type:ty) => {
        $crate::some_function_impl!(@step $func_name, $ret_type, [], [], []; y; n;
            [(arg0, ($arg_type0)), (arg1, ($arg_type1)), (arg2, ($arg_type2))]
        );
    };
    ($func_name:ident [$($tgen:tt)*], $arg_type0:ty, $arg_type1:ty, $arg_type2:ty, $ret_type:ty) => {
        $crate::some_function_impl!(@step $func_name, $ret_type, [$($tgen)*], [], []; y; n;
            [(arg0, ($arg_type0)), (arg1, ($arg_type1)), (arg2, ($arg_type2))]
        );
    };
}

/// Macro to create variants of a function with 4 arguments.
/// If there exists a function `f____(x: T, y: S, z: V, w: W) -> U`, this
/// creates the 15 nullable variants (all combinations except all-non-nullable).
/// Each generated function returns `None` when any argument is `None`.
#[doc(hidden)]
#[macro_export]
macro_rules! some_function4 {
    ($func_name:ident, $arg_type0:ty, $arg_type1:ty, $arg_type2:ty, $arg_type3:ty, $ret_type:ty) => {
        $crate::some_function_impl!(@step $func_name, $ret_type, [], [], []; y; n;
            [(arg0, ($arg_type0)), (arg1, ($arg_type1)), (arg2, ($arg_type2)), (arg3, ($arg_type3))]
        );
    };
    ($func_name:ident [$($tgen:tt)*], $arg_type0:ty, $arg_type1:ty, $arg_type2:ty, $arg_type3:ty, $ret_type:ty) => {
        $crate::some_function_impl!(@step $func_name, $ret_type, [$($tgen)*], [], []; y; n;
            [(arg0, ($arg_type0)), (arg1, ($arg_type1)), (arg2, ($arg_type2)), (arg3, ($arg_type3))]
        );
    };
}

/// Like [`some_function1`] but for a base function that already returns `Option<R>`.
/// If there exists `f_(x: T) -> Option<S>`, creates:
/// - `fN(x: Option<T>) -> Option<S>`
#[doc(hidden)]
#[macro_export]
macro_rules! some_nullable_function1 {
    ($func_name:ident, $arg_type:ty, $ret_type:ty) => {
        $crate::some_function_impl!(@step $func_name, $ret_type, [], [], []; n; n;
            [(arg0, ($arg_type))]
        );
    };
    ($func_name:ident [$($tgen:tt)*], $arg_type:ty, $ret_type:ty) => {
        $crate::some_function_impl!(@step $func_name, $ret_type, [$($tgen)*], [], []; n; n;
            [(arg0, ($arg_type))]
        );
    };
}

/// Like [`some_function2`] but for a base function that already returns `Option<R>`.
/// If there exists `f__(x: T, y: S) -> Option<U>`, creates:
/// - `f_N`, `fN_`, `fNN`
#[doc(hidden)]
#[macro_export]
macro_rules! some_nullable_function2 {
    ($func_name:ident, $arg_type0:ty, $arg_type1:ty, $ret_type:ty) => {
        $crate::some_function_impl!(@step $func_name, $ret_type, [], [], []; n; n;
            [(arg0, ($arg_type0)), (arg1, ($arg_type1))]
        );
    };
    ($func_name:ident [$($tgen:tt)*], $arg_type0:ty, $arg_type1:ty, $ret_type:ty) => {
        $crate::some_function_impl!(@step $func_name, $ret_type, [$($tgen)*], [], []; n; n;
            [(arg0, ($arg_type0)), (arg1, ($arg_type1))]
        );
    };
}

/// Like [`some_function3`] but for a base function that already returns `Option<R>`.
/// If there exists `f___(x: T, y: S, z: V) -> Option<U>`, creates the 7 nullable variants.
#[doc(hidden)]
#[macro_export]
macro_rules! some_nullable_function3 {
    ($func_name:ident, $arg_type0:ty, $arg_type1:ty, $arg_type2:ty, $ret_type:ty) => {
        $crate::some_function_impl!(@step $func_name, $ret_type, [], [], []; n; n;
            [(arg0, ($arg_type0)), (arg1, ($arg_type1)), (arg2, ($arg_type2))]
        );
    };
    ($func_name:ident [$($tgen:tt)*], $arg_type0:ty, $arg_type1:ty, $arg_type2:ty, $ret_type:ty) => {
        $crate::some_function_impl!(@step $func_name, $ret_type, [$($tgen)*], [], []; n; n;
            [(arg0, ($arg_type0)), (arg1, ($arg_type1)), (arg2, ($arg_type2))]
        );
    };
}

/// Like [`some_function4`] but for a base function that already returns `Option<R>`.
/// If there exists `f____(x: T, y: S, z: V, w: W) -> Option<U>`, creates the 15 nullable variants.
#[doc(hidden)]
#[macro_export]
macro_rules! some_nullable_function4 {
    ($func_name:ident, $arg_type0:ty, $arg_type1:ty, $arg_type2:ty, $arg_type3:ty, $ret_type:ty) => {
        $crate::some_function_impl!(@step $func_name, $ret_type, [], [], []; n; n;
            [(arg0, ($arg_type0)), (arg1, ($arg_type1)), (arg2, ($arg_type2)), (arg3, ($arg_type3))]
        );
    };
    ($func_name:ident [$($tgen:tt)*], $arg_type0:ty, $arg_type1:ty, $arg_type2:ty, $arg_type3:ty, $ret_type:ty) => {
        $crate::some_function_impl!(@step $func_name, $ret_type, [$($tgen)*], [], []; n; n;
            [(arg0, ($arg_type0)), (arg1, ($arg_type1)), (arg2, ($arg_type2)), (arg3, ($arg_type3))]
        );
    };
}

/// Macro to create variants of a function with 2 arguments
/// optimized for the implementation of arithmetic operators.
/// Assuming there exists a function is f__(x: T, y: T) -> U, this creates
/// three functions:
/// - f_tN_t(x: T, y: Option<T>) -> Option<U>
/// - f_t_tN(x: Option<T>, y: T) -> Option<U>
/// - f_tN_tN(x: Option<T>, y: Option<T>) -> Option<U>
///
/// The resulting functions return Some only if all arguments are 'Some'.
macro_rules! some_existing_operator {
    ($func_name: ident $(< $( const $var:ident : $ty: ty),* >)?, $short_name: ident, $arg_type: ty, $ret_type: ty) => {
        ::paste::paste! {
            #[inline(always)]
            #[doc(hidden)]
            pub fn [<$func_name _ $short_name N _ $short_name N>] $(< $( const $var : $ty ),* >)? ( arg0: Option<$arg_type>, arg1: Option<$arg_type> ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg1 = arg1?;
                Some([<$func_name _ $short_name _ $short_name>] $(:: < $($var),* >)? (arg0, arg1))
            }

            #[inline(always)]
            #[doc(hidden)]
            pub fn [<$func_name _ $short_name _ $short_name N>] $(< $( const $var : $ty ),* >)? ( arg0: $arg_type, arg1: Option<$arg_type> ) -> Option<$ret_type> {
                let arg1 = arg1?;
                Some([<$func_name _ $short_name _ $short_name>] $(:: < $($var),* >)? (arg0, arg1))
            }

            #[inline(always)]
            #[doc(hidden)]
            pub fn [<$func_name _ $short_name N _ $short_name>] $(< $( const $var : $ty ),* >)? ( arg0: Option<$arg_type>, arg1: $arg_type ) -> Option<$ret_type> {
                let arg0 = arg0?;
                Some([<$func_name _ $short_name _ $short_name>] $(:: < $($var),* >)? (arg0, arg1))
            }
        }
    }
}

pub(crate) use some_existing_operator;

/// Macro to create variants of a function with 2 arguments
/// optimized for the implementation of arithmetic operators.
/// Assuming there exists a function is f(x: T, y: T) -> U, this creates
/// four functions:
/// - f_t_t(x: T, y: T) -> U
/// - f_tN_t(x: T, y: Option<T>) -> Option<U>
/// - f_t_tN(x: Option<T>, y: T) -> Option<U>
/// - f_tN_tN(x: Option<T>, y: Option<T>) -> Option<U>
///
/// The resulting functions return Some only if all arguments are 'Some'.
///
/// Has two variants:
/// - Takes the name of the existing function, the generated functions will have
///   this prefix
/// - Takes the name of the existing function, and the prefix for the generated
///   functions
macro_rules! some_operator {
    ($func_name: ident $(< $( const $var:ident : $ty: ty),* >)?, $short_name: ident, $arg_type: ty, $ret_type: ty) => {
        ::paste::paste! {
            #[doc(hidden)]
            #[inline(always)]
            pub fn [<$func_name _ $short_name _ $short_name >] $(< $(const $var : $ty),* >)?  ( arg0: $arg_type, arg1: $arg_type ) -> $ret_type {
                $func_name $(:: < $($var),* >)? (arg0, arg1)
            }

            some_existing_operator!($func_name $(< $( const $var : $ty),* >)?, $short_name, $arg_type, $ret_type);
        }
    };

    ($func_name: ident $(< $( const $var:ident : $ty: ty),* >)?, $new_func_name: ident, $short_name: ident, $arg_type: ty, $ret_type: ty) => {
        ::paste::paste! {
            #[doc(hidden)]
            #[inline(always)]
            pub fn [<$new_func_name _ $short_name _ $short_name >] $(< $(const $var : $ty ),* >)? ( arg0: $arg_type, arg1: $arg_type ) -> $ret_type {
                $func_name $(:: < $($var),* >)? (arg0, arg1)
            }

            some_existing_operator!($new_func_name $(< $( const $var : $ty),* >)?, $short_name, $arg_type, $ret_type);
        }
    }
}

pub(crate) use some_operator;

#[doc(hidden)]
#[inline(always)]
pub fn is_null<T>(value: Option<T>) -> bool {
    value.is_none()
}

#[doc(hidden)]
#[inline(always)]
pub fn indicator<T, R>(value: &Option<T>) -> R
where
    R: HasZero + HasOne,
{
    match value {
        None => R::zero(),
        Some(_) => R::one(),
    }
}

//////////////////////////////////////////

#[doc(hidden)]
#[inline(always)]
pub fn abs_i8(left: i8) -> i8 {
    left.abs()
}

#[doc(hidden)]
#[inline(always)]
pub fn abs_i16(left: i16) -> i16 {
    left.abs()
}

#[doc(hidden)]
#[inline(always)]
pub fn abs_i32(left: i32) -> i32 {
    left.abs()
}

#[doc(hidden)]
#[inline(always)]
pub fn abs_i64(left: i64) -> i64 {
    left.abs()
}

#[doc(hidden)]
#[inline(always)]
pub fn abs_i128(left: i128) -> i128 {
    left.abs()
}

#[doc(hidden)]
#[inline(always)]
pub fn abs_f(left: F32) -> F32 {
    left.abs()
}

#[doc(hidden)]
#[inline(always)]
pub fn abs_d(left: F64) -> F64 {
    left.abs()
}

#[doc(hidden)]
#[inline(always)]
pub fn abs_u8(left: u8) -> u8 {
    left
}

#[doc(hidden)]
#[inline(always)]
pub fn abs_u16(left: u16) -> u16 {
    left
}

#[doc(hidden)]
#[inline(always)]
pub fn abs_u32(left: u32) -> u32 {
    left
}

#[doc(hidden)]
#[inline(always)]
pub fn abs_u64(left: u64) -> u64 {
    left
}

#[doc(hidden)]
#[inline(always)]
pub fn abs_u128(left: u128) -> u128 {
    left
}

some_polymorphic_function1!(abs, i8, i8, i8);
some_polymorphic_function1!(abs, i16, i16, i16);
some_polymorphic_function1!(abs, i32, i32, i32);
some_polymorphic_function1!(abs, i64, i64, i64);
some_polymorphic_function1!(abs, i128, i128, i128);
some_polymorphic_function1!(abs, f, F32, F32);
some_polymorphic_function1!(abs, d, F64, F64);
some_polymorphic_function1!(abs, u8, u8, u8);
some_polymorphic_function1!(abs, u16, u16, u16);
some_polymorphic_function1!(abs, u32, u32, u32);
some_polymorphic_function1!(abs, u64, u64, u64);
some_polymorphic_function1!(abs, u128, u128, u128);

#[inline(always)]
#[doc(hidden)]
pub fn sign_i8(x: i8) -> i8 {
    if x == 0 { x } else { x.signum() }
}

#[inline(always)]
#[doc(hidden)]
pub fn sign_i16(x: i16) -> i16 {
    if x == 0 { x } else { x.signum() }
}

#[inline(always)]
#[doc(hidden)]
pub fn sign_i32(x: i32) -> i32 {
    if x == 0 { x } else { x.signum() }
}

#[inline(always)]
#[doc(hidden)]
pub fn sign_i64(x: i64) -> i64 {
    if x == 0 { x } else { x.signum() }
}

#[inline(always)]
#[doc(hidden)]
pub fn sign_i128(x: i128) -> i128 {
    if x == 0 { x } else { x.signum() }
}

#[inline(always)]
#[doc(hidden)]
pub fn sign_u8(x: u8) -> u8 {
    if x == 0 { x } else { 1 }
}

#[inline(always)]
#[doc(hidden)]
pub fn sign_u16(x: u16) -> u16 {
    if x == 0 { x } else { 1 }
}

#[inline(always)]
#[doc(hidden)]
pub fn sign_u32(x: u32) -> u32 {
    if x == 0 { x } else { 1 }
}

#[inline(always)]
#[doc(hidden)]
pub fn sign_u64(x: u64) -> u64 {
    if x == 0 { x } else { 1 }
}

#[inline(always)]
#[doc(hidden)]
pub fn sign_u128(x: u128) -> u128 {
    if x == 0 { x } else { 1 }
}

some_polymorphic_function1!(sign, i8, i8, i8);
some_polymorphic_function1!(sign, i16, i16, i16);
some_polymorphic_function1!(sign, i32, i32, i32);
some_polymorphic_function1!(sign, i64, i64, i64);
some_polymorphic_function1!(sign, i128, i128, i128);
some_polymorphic_function1!(sign, u8, u8, u8);
some_polymorphic_function1!(sign, u16, u16, u16);
some_polymorphic_function1!(sign, u32, u32, u32);
some_polymorphic_function1!(sign, u64, u64, u64);
some_polymorphic_function1!(sign, u128, u128, u128);

#[doc(hidden)]
#[inline(always)]
pub fn is_distinct__<T>(left: T, right: T) -> bool
where
    T: Eq,
{
    left != right
}

#[doc(hidden)]
#[inline(always)]
pub fn is_distinct_N_N<T>(left: Option<T>, right: Option<T>) -> bool
where
    T: Eq,
{
    left != right
}

#[doc(hidden)]
#[inline(always)]
pub fn is_distinct__N<T>(left: T, right: Option<T>) -> bool
where
    T: Eq,
{
    match right {
        Some(b) => left != b,
        None => true,
    }
}

#[doc(hidden)]
#[inline(always)]
pub fn is_distinct_N_<T>(left: Option<T>, right: T) -> bool
where
    T: Eq,
{
    match left {
        Some(a) => a != right,
        None => true,
    }
}

#[doc(hidden)]
pub fn weighted_push<T, W>(vec: &mut Vec<T>, value: &T, weight: W)
where
    W: ZRingValue,
    T: Clone,
{
    let mut w = weight;
    let negone = W::one().neg();
    while w != W::zero() {
        vec.push(value.clone());
        w = w.add_by_ref(&negone);
    }
}

#[doc(hidden)]
pub fn power_i32_i32(left: i32, right: i32) -> F64 {
    (left as f64).pow(right).into()
}

some_polymorphic_function2!(power, i32, i32, i32, i32, F64);

////////////////////////////////////////////////

// Functions called by 'writelog'
#[doc(hidden)]
pub fn dump<T>(prefix: SqlString, data: &T) -> T
where
    T: Debug + Clone,
{
    println!("{}: {:?}", prefix.str(), data);
    data.clone()
}

#[doc(hidden)]
pub fn print(str: SqlString) {
    print!("{}", str.str())
}

#[doc(hidden)]
pub fn print_opt(str: Option<SqlString>) {
    match str {
        None => print!("NULL"),
        Some(x) => print!("{}", x.str()),
    }
}

#[doc(hidden)]
pub fn zset_map<D, T, F>(data: &WSet<D>, mapper: F) -> WSet<T>
where
    D: DBData + 'static,
    T: DBData + 'static,
    F: Fn(&D) -> T,
{
    let mut tuples = Vec::new();
    let mut cursor = data.cursor();
    while cursor.key_valid() {
        let item = unsafe { cursor.key().downcast::<D>() };
        let data = mapper(item);
        let weight = unsafe { *cursor.weight().downcast::<ZWeight>() };
        tuples.push(Tup2(Tup2(data, ()), weight));
        cursor.step_key();
    }
    WSet::from_tuples((), tuples)
}

#[doc(hidden)]
pub fn late() {
    TOTAL_LATE_RECORDS.fetch_add(1, Ordering::Relaxed);
}

#[doc(hidden)]
pub fn zset_filter_comparator<D, T, F>(data: &WSet<D>, value: &T, comparator: F) -> WSet<D>
where
    D: DBData + 'static,
    T: 'static + Debug,
    F: Fn(&D, &T) -> bool,
{
    let factories = OrdZSetFactories::new::<D, (), ZWeight>();

    let mut builder = OrdWSetBuilder::with_capacity(&factories, data.len(), data.len());

    let mut cursor = data.cursor();
    while cursor.key_valid() {
        let item = unsafe { cursor.key().downcast::<D>() };
        if comparator(item, value) {
            // println!("controlled_filter accepts={:?}", item);
            builder.push_val_diff(().erase(), cursor.weight());
            builder.push_key(cursor.key());
        } else {
            late();
        }
        cursor.step_key();
    }
    TypedBatch::new(builder.done())
}

#[doc(hidden)]
pub fn indexed_zset_filter_comparator<K, D, T, F>(
    data: &IndexedWSet<K, D>,
    value: &T,
    comparator: F,
) -> IndexedWSet<K, D>
where
    K: DBData + Erase<DynData>,
    D: DBData + Erase<DynData>,
    T: 'static,
    F: Fn((&K, &D), &T) -> bool,
{
    let factories = OrdIndexedZSetFactories::new::<K, D, ZWeight>();
    let mut builder =
        OrdIndexedWSetBuilder::with_capacity(&factories, data.key_count(), data.len());

    let mut cursor = data.cursor();
    while cursor.key_valid() {
        let key = unsafe { cursor.key().downcast::<K>() }.clone();
        let mut any_values = false;
        while cursor.val_valid() {
            let w = *cursor.weight().deref();
            let item = unsafe { cursor.val().downcast::<D>() };
            if comparator((&key, item), value) {
                builder.push_val_diff(item.erase(), w.erase());
                any_values = true;
            } else {
                late();
            }
            cursor.step_val();
        }
        if any_values {
            builder.push_key(cursor.key());
        }
        cursor.step_key();
    }
    TypedBatch::new(builder.done())
}

#[doc(hidden)]
pub fn append_to_map_handle<K, V, U>(
    data: &WSet<V>,
    handle: &MapHandle<K, V, U>,
    key_f: fn(&V) -> K,
) where
    K: DBData,
    V: DBData,
    U: DBData,
{
    let mut cursor = data.cursor();
    while cursor.key_valid() {
        let w = *cursor.weight().deref();
        if w.is_zero() {
            continue;
        }
        if !w.ge0() {
            let key = unsafe { cursor.key().downcast::<V>() };
            handle.push(key_f(&key.clone()), Update::Delete);
        } else {
            let key = unsafe { cursor.key().downcast::<V>() };
            handle.push(key_f(&key.clone()), Update::Insert(key.clone()));
        }
        cursor.step_key();
    }
}

#[doc(hidden)]
pub fn append_to_collection_handle<K>(data: &WSet<K>, handle: &ZSetHandle<K>)
where
    K: DBData,
{
    let mut cursor = data.cursor();
    while cursor.key_valid() {
        handle.push(
            unsafe { cursor.key().downcast::<K>() }.clone(),
            *cursor.weight().deref(),
        );
        cursor.step_key();
    }
}

#[doc(hidden)]
pub fn read_output_handle<K>(handle: &OutputHandle<WSet<K>>) -> WSet<K>
where
    K: DBData,
{
    handle.consolidate()
}

#[doc(hidden)]
pub fn read_output_spine<K>(handle: &OutputHandle<SpineSnapshot<WSet<K>>>) -> WSet<K>
where
    K: DBData,
{
    handle.concat().consolidate()
}

// Check that two zsets are equal.  If yes, returns true.
// If not, print a diff of the zsets and returns false.
// Assumes that the zsets are positive (all weights are positive).
#[doc(hidden)]
pub fn must_equal<K>(left: &WSet<K>, right: &WSet<K>) -> bool
where
    K: DBData + Clone,
{
    // println!("L={:?}\nR={:?}\n", left, right);
    let diff = left.add_by_ref(&right.neg_by_ref());
    if diff.is_zero() {
        return true;
    }
    let mut cursor = diff.cursor();
    let mut shown = 0;
    let mut left = 0;
    let mut right = 0;
    while cursor.key_valid() {
        let weight = **cursor.weight();
        let key = cursor.key();
        if shown < 50 {
            if weight.le0() {
                println!("R: {:?}x{:?}", key, weight.neg());
            } else {
                println!("L: {:?}x{:?}", key, weight);
            }
        } else if weight.le0() {
            right += weight.neg();
        } else {
            left += weight;
        }
        cursor.step_key();
        shown += 1;
    }
    if left > 0 || right > 0 {
        println!("Additional L:{left} and R:{right} rows not shown");
    }
    false
}

#[doc(hidden)]
pub fn zset_size<K>(set: &WSet<K>) -> i64 {
    let mut w = 0;
    let mut cursor = set.cursor();
    while cursor.key_valid() {
        let weight = **cursor.weight();
        w += weight;
        cursor.step_key();
    }
    w
}

//////////////////////// Semigroup implementations

#[derive(Clone)]
#[doc(hidden)]
pub struct DefaultOptSemigroup<T>(PhantomData<T>);

#[doc(hidden)]
impl<T> Semigroup<Option<T>> for DefaultOptSemigroup<T>
where
    T: SemigroupValue,
{
    #[doc(hidden)]
    fn combine(left: &Option<T>, right: &Option<T>) -> Option<T> {
        match (left, right) {
            (None, _) => None,
            (_, None) => None,
            (Some(x), Some(y)) => Some(x.add_by_ref(y)),
        }
    }
}

#[derive(Clone)]
#[doc(hidden)]
pub struct PairSemigroup<T, R, TS, RS>(PhantomData<(T, R, TS, RS)>);

#[doc(hidden)]
impl<T, R, TS, RS> Semigroup<Tup2<T, R>> for PairSemigroup<T, R, TS, RS>
where
    TS: Semigroup<T>,
    RS: Semigroup<R>,
{
    #[doc(hidden)]
    fn combine(left: &Tup2<T, R>, right: &Tup2<T, R>) -> Tup2<T, R> {
        Tup2::new(
            TS::combine(&left.0, &right.0),
            RS::combine(&left.1, &right.1),
        )
    }
}

#[derive(Clone)]
#[doc(hidden)]
pub struct TripleSemigroup<T, R, V, TS, RS, VS>(PhantomData<(T, R, V, TS, RS, VS)>);

#[doc(hidden)]
impl<T, R, V, TS, RS, VS> Semigroup<Tup3<T, R, V>> for TripleSemigroup<T, R, V, TS, RS, VS>
where
    TS: Semigroup<T>,
    RS: Semigroup<R>,
    VS: Semigroup<V>,
{
    #[doc(hidden)]
    fn combine(left: &Tup3<T, R, V>, right: &Tup3<T, R, V>) -> Tup3<T, R, V> {
        Tup3::new(
            TS::combine(&left.0, &right.0),
            RS::combine(&left.1, &right.1),
            VS::combine(&left.2, &right.2),
        )
    }
}

#[doc(hidden)]
#[derive(Clone)]
pub struct ConcatSemigroup<V>(PhantomData<V>);

// Semigroup for the SINGLE_VALUE aggregate
#[derive(Clone)]
#[doc(hidden)]
pub struct SingleSemigroup<T>(PhantomData<T>);

#[doc(hidden)]
impl<T> Semigroup<Tup2<bool, T>> for SingleSemigroup<Tup2<bool, T>>
where
    T: Clone,
{
    #[doc(hidden)]
    fn combine(left: &Tup2<bool, T>, right: &Tup2<bool, T>) -> Tup2<bool, T> {
        if left.0 && right.0 {
            panic!("More than one value in subquery");
        }
        Tup2::new(
            left.0 || right.0,
            if left.0 {
                left.1.clone()
            } else {
                right.1.clone()
            },
        )
    }
}

/// Semigroup for the an aggregate which computes nothing
/// Useful when the compiler removes all aggregates from an aggregate operator
/// (This is currently never used, because the compiler generates a linear aggregate
/// for this case)
#[derive(Clone)]
#[doc(hidden)]
pub struct EmptySemigroup;

#[doc(hidden)]
impl Semigroup<Tup0> for EmptySemigroup {
    #[doc(hidden)]
    fn combine(_left: &Tup0, _right: &Tup0) -> Tup0 {
        Tup0::new()
    }
}

/// A lazily initialized static value, where the initialized can be a capturing
/// closure.
///
/// This struct is supposed to do the same thing as some other lazy
/// Rust types, like std::sync::Lazy, or std::cell::LazyCell: it holds
/// a constant value, but which needs to be initialized only on demand
/// (because it could panic).  Unfortunately I could not use an
/// off-the-shelf struct like the ones mentioned, because the
/// initializer is not always `const`.  This implementation separates
/// the creation (`new`) from the initialization (`init`).  This
/// enables us to use this class to declare `static` Rust values,
/// which nevertheless are initialized on first use.  In the
/// initializers we actually always use static constant values to, but
/// they are sometimes passed through function arguments, so the Rust
/// compiler cannot recognize them as such.  For example:
/// ```ignore
/// // In the main crate:
/// pub static SOURCE_MAP_STATIC: OnceLock<SourceMap> = OnceLock::new();
/// SOURCE_MAP_STATIC.get_or_init(|| {
///      let mut m = SourceMap::new();
///           m.insert("0ee9907f", 0, SourcePosition::new(222, 2));
///           m.insert("0ee9907f", 1, SourcePosition::new(223, 2));
///           m
///       });
/// let sourceMap = SOURCE_MAP_STATIC.get().unwrap();
/// let s3 = create_operator_x(&circuit, Some("0ee9907f"), &sourceMap, &mut catalog, s2);
/// ...
/// // In the operator crate
/// pub fn create_operator_x(
///         circuit: &RootCircuit,
///         hash: Option<&'static str>,
///         sourceMap: &'static SourceMap,  // static lifetime, but not const
///         catalog: &mut Catalog,
///         i0: &Stream<RootCircuit, WSet<...>>) -> Stream<RootCircuit, ...> {
///    static STATIC_4dbc5aebe9c4edea: StaticLazy<Option<SqlDecimal<38, 6>>> = StaticLazy::new();
///    STATIC_4dbc5aebe9c4edea.init(move || handle_error_with_position(
///               "global", 25, &sourceMap, cast_to_SqlDecimalN_i32N::<38, 6>(Some(1i32));
///    ...
/// };
/// ```
/// The function `handle_error_with_position` may panic, so it needs to be lazily evaluated.
#[derive(Default)]
#[doc(hidden)]
pub struct StaticLazy<T: 'static> {
    cell: OnceLock<T>,
    init: OnceLock<&'static (dyn Fn() -> T + Send + Sync)>,
}

#[doc(hidden)]
impl<T> StaticLazy<T> {
    #[doc(hidden)]
    pub const fn new() -> Self {
        Self {
            cell: OnceLock::new(),
            init: OnceLock::new(),
        }
    }

    #[doc(hidden)]
    pub fn init<F>(&'static self, f: F)
    where
        F: Fn() -> T + Send + Sync + 'static,
    {
        if self.cell.get().is_some() {
            return;
        }
        let leaked: &'static (dyn Fn() -> T + Send + Sync) = Box::leak(Box::new(f));
        self.init.set(leaked).unwrap_or(());
    }

    #[doc(hidden)]
    fn get(&self) -> &T {
        self.cell.get_or_init(|| {
            let f = self
                .init
                .get()
                .unwrap_or_else(|| panic!("Initializer not set"));
            f()
        })
    }
}

#[doc(hidden)]
impl<T> Deref for StaticLazy<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.get()
    }
}
