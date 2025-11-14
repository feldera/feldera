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
    algebra::{
        AddByRef, HasOne, HasZero, NegByRef, OrdIndexedZSetFactories, OrdZSetFactories, Semigroup,
        SemigroupValue, ZRingValue,
    },
    circuit::metrics::TOTAL_LATE_RECORDS,
    dynamic::{DowncastTrait, DynData, Erase},
    operator::Update,
    trace::{
        ord::{OrdIndexedWSetBuilder, OrdWSetBuilder},
        BatchReader, BatchReaderFactories, Builder, Cursor,
    },
    typed_batch::{SpineSnapshot, TypedBatch},
    utils::*,
    DBData, MapHandle, OrdIndexedZSet, OrdZSet, OutputHandle, ZSetHandle, ZWeight,
};
use num::PrimInt;
use num_traits::Pow;
use std::marker::PhantomData;
use std::ops::{Deref, Neg};
use std::sync::OnceLock;
use std::{fmt::Debug, sync::atomic::Ordering};

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

// Macro to create variants of a function with 1 argument
// If there exists a function is f_(x: T) -> S, this creates a function
// fN(x: Option<T>) -> Option<S>, defined as
// fN(x) { let x = x?; Some(f_(x)) }.
macro_rules! some_function1 {
    ($func_name:ident, $arg_type:ty, $ret_type:ty) => {
        ::paste::paste! {
            #[doc(hidden)]
            pub fn [<$func_name N>]( arg: Option<$arg_type> ) -> Option<$ret_type> {
                let arg = arg?;
                Some([<$func_name _>](arg))
            }
        }
    };
}

pub(crate) use some_function1;

// Macro to create variants of a function with 1 argument
// If there exists a function is f_type(x: T) -> S, this creates a function
// f_typeN(x: Option<T>) -> Option<S>
// { let x = x?; Some(f_type(x)) }.
macro_rules! some_polymorphic_function1 {
    ($func_name:ident $(< $( const $var : ident : $ty: ty),* >)?, $type_name: ident, $arg_type:ty, $ret_type:ty) => {
        ::paste::paste! {
            #[doc(hidden)]
            pub fn [<$func_name _ $type_name N>] $(< $( const $var : $ty ),* >)? ( arg: Option<$arg_type> ) -> Option<$ret_type> {
                let arg = arg?;
                Some([<$func_name _ $type_name >] $(:: < $($var),* >)? (arg))
            }
        }
    };
}

pub(crate) use some_polymorphic_function1;

// Macro to create variants of a polymorphic function with 1 argument that is
// also polymorphic in the return type.
// If there exists a function is f_type_result(x: T) -> S, this creates a
// function
// f_typeN_resultN(x: Option<T>) -> Option<S>
// { let x = x?; Some(f_type(x)) }.
#[allow(unused_macros)]
macro_rules! polymorphic_return_function1 {
    ($func_name:ident, $type_name: ident, $arg_type:ty, $ret_name: ident, $ret_type:ty) => {
        ::paste::paste! {
            #[doc(hidden)]
            pub fn [<$func_name _ $type_name N _ $ret_name N>]( arg: Option<$arg_type> ) -> Option<$ret_type> {
                let arg = arg?;
                Some([<$func_name _ $type_name >](arg))
            }
        }
    };
}

// Maybe we will need this someday
#[allow(unused_imports)]
pub(crate) use polymorphic_return_function1;

// Macro to create variants of a function with 2 arguments
// If there exists a function is f__(x: T, y: S) -> U, this creates
// three functions:
// - f_N(x: T, y: Option<S>) -> Option<U>
// - fN_(x: Option<T>, y: S) -> Option<U>
// - fNN(x: Option<T>, y: Option<S>) -> Option<U>
// The resulting functions return Some only if all arguments are 'Some'.
macro_rules! some_function2 {
    ($func_name:ident, $arg_type0:ty, $arg_type1:ty, $ret_type:ty) => {
        ::paste::paste! {
            #[doc(hidden)]
            pub fn [<$func_name NN>]( arg0: Option<$arg_type0>, arg1: Option<$arg_type1> ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg1 = arg1?;
                Some([<$func_name __>](arg0, arg1))
            }

            #[doc(hidden)]
            pub fn [<$func_name _N>]( arg0: $arg_type0, arg1: Option<$arg_type1> ) -> Option<$ret_type> {
                let arg1 = arg1?;
                Some([<$func_name __>](arg0, arg1))
            }

            #[doc(hidden)]
            pub fn [<$func_name N_>]( arg0: Option<$arg_type0>, arg1: $arg_type1 ) -> Option<$ret_type> {
                let arg0 = arg0?;
                Some([<$func_name __>](arg0, arg1))
            }
        }
    }
}

pub(crate) use some_function2;

// Macro to create variants of a polymorphic function with 2 arguments
// that is also polymorphic in the return type
// If there exists a function is f_type1_type2_result(x: T, y: S) -> U, this
// creates three functions:
// - f_type1_type2N_resultN(x: T, y: Option<S>) -> Option<U>
// - f_type1N_type2_resultN(x: Option<T>, y: S) -> Option<U>
// - f_type1N_type2N_resultN(x: Option<T>, y: Option<S>) -> Option<U>
// The resulting functions return Some only if all arguments are 'Some'.
macro_rules! polymorphic_return_function2 {
    ($func_name:ident, $type_name0: ident, $arg_type0:ty, $type_name1: ident, $arg_type1:ty, $ret_name: ident, $ret_type:ty) => {
        ::paste::paste! {
            #[doc(hidden)]
            pub fn [<$func_name _$type_name0 _ $type_name1 N _ $ret_name N>]( arg0: $arg_type0, arg1: Option<$arg_type1> ) -> Option<$ret_type> {
                let arg1 = arg1?;
                Some([<$func_name _ $type_name0 _ $type_name1 _ $ret_name>](arg0, arg1))
            }

            #[doc(hidden)]
            pub fn [<$func_name _ $type_name0 N _ $type_name1 _ $ret_name N>]( arg0: Option<$arg_type0>, arg1: $arg_type1 ) -> Option<$ret_type> {
                let arg0 = arg0?;
                Some([<$func_name _ $type_name0 _ $type_name1 _ $ret_name>](arg0, arg1))
            }

            #[doc(hidden)]
            pub fn [<$func_name _ $type_name0 N _ $type_name1 N _ $ret_name N>]( arg0: Option<$arg_type0>, arg1: Option<$arg_type1> ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg1 = arg1?;
                Some([<$func_name _ $type_name0 _ $type_name1 _ $ret_name>](arg0, arg1))
            }
        }
    }
}

pub(crate) use polymorphic_return_function2;

// Macro to create variants of a polymorphic function with 2 arguments
// If there exists a function is f_type1_type2(x: T, y: S) -> U, this
// creates three functions:
// - f_type1_type2N(x: T, y: Option<S>) -> Option<U>
// - f_type1N_type2(x: Option<T>, y: S) -> Option<U>
// - f_type1N_type2N(x: Option<T>, y: Option<S>) -> Option<U>
// The resulting functions return Some only if all arguments are 'Some'.
macro_rules! some_polymorphic_function2 {
    ($func_name:ident  $(< $( const $var:ident : $ty: ty),* >)?, $type_name0: ident, $arg_type0:ty, $type_name1: ident, $arg_type1:ty, $ret_type:ty) => {
        ::paste::paste! {
            #[doc(hidden)]
            pub fn [<$func_name _$type_name0 _ $type_name1 N>] $(< $( const $var : $ty),* >)? ( arg0: $arg_type0, arg1: Option<$arg_type1> ) -> Option<$ret_type> {
                let arg1 = arg1?;
                Some([<$func_name _ $type_name0 _ $type_name1>] $(:: < $($var),* >)? (arg0, arg1))
            }

            #[doc(hidden)]
            pub fn [<$func_name _ $type_name0 N _ $type_name1>] $(< $( const $var : $ty),* >)? ( arg0: Option<$arg_type0>, arg1: $arg_type1 ) -> Option<$ret_type> {
                let arg0 = arg0?;
                Some([<$func_name _ $type_name0 _ $type_name1>] $(:: < $($var),* >)? (arg0, arg1))
            }

            #[doc(hidden)]
            pub fn [<$func_name _ $type_name0 N _ $type_name1 N>] $(< $( const $var : $ty),* >)? ( arg0: Option<$arg_type0>, arg1: Option<$arg_type1> ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg1 = arg1?;
                Some([<$func_name _ $type_name0 _ $type_name1>] $(:: < $($var),* >)? (arg0, arg1))
            }
        }
    }
}

pub(crate) use some_polymorphic_function2;

// If there exists a function is f_t1_t2_t3(x: T, y: S, z: V) -> U, this creates
// seven functions:
// - f_t1_t2_t3N(x: T, y: S, z: Option<V>) -> Option<U>
// - f_t1_t2N_t2(x: T, y: Option<S>, z: V) -> Option<U>
// - etc.
// The resulting functions return Some only if all arguments are 'Some'.
macro_rules! some_polymorphic_function3 {
    ($func_name:ident,
     $type_name0: ident, $arg_type0:ty,
     $type_name1: ident, $arg_type1:ty,
     $type_name2: ident, $arg_type2: ty,
     $ret_type:ty) => {
        ::paste::paste! {
            #[doc(hidden)]
            pub fn [<$func_name _ $type_name0 _ $type_name1 _ $type_name2 N>](
                arg0: $arg_type0,
                arg1: $arg_type1,
                arg2: Option<$arg_type2>
            ) -> Option<$ret_type> {
                let arg2 = arg2?;
                Some([<$func_name _ $type_name0 _ $type_name1 _ $type_name2>](arg0, arg1, arg2))
            }

            #[doc(hidden)]
            pub fn [<$func_name _ $type_name0 _ $type_name1 N _ $type_name2>](
                arg0: $arg_type0,
                arg1: Option<$arg_type1>,
                arg2: $arg_type2
            ) -> Option<$ret_type> {
                let arg1 = arg1?;
                Some([<$func_name _ $type_name0 _ $type_name1 _ $type_name2>](arg0, arg1, arg2))
            }

            #[doc(hidden)]
            pub fn [<$func_name _ $type_name0 _ $type_name1 N _ $type_name2 N>](
                arg0: $arg_type0,
                arg1: Option<$arg_type1>,
                arg2: Option<$arg_type2>
            ) -> Option<$ret_type> {
                let arg1 = arg1?;
                let arg2 = arg2?;
                Some([<$func_name _ $type_name0 _ $type_name1 _ $type_name2>](arg0, arg1, arg2))
            }

            #[doc(hidden)]
            pub fn [<$func_name _ $type_name0 N _ $type_name1 _ $type_name2>](
                arg0: Option<$arg_type0>,
                arg1: $arg_type1,
                arg2: $arg_type2
            ) -> Option<$ret_type> {
                let arg0 = arg0?;
                Some([<$func_name _ $type_name0 _ $type_name1 _ $type_name2>](arg0, arg1, arg2))
            }

            #[doc(hidden)]
            pub fn [<$func_name _ $type_name0 N _ $type_name1 _ $type_name2 N>](
                arg0: Option<$arg_type0>,
                arg1: $arg_type1,
                arg2: Option<$arg_type2>
            ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg2 = arg2?;
                Some([<$func_name _ $type_name0 _ $type_name1 _ $type_name2>](arg0, arg1, arg2))
            }

            #[doc(hidden)]
            pub fn [<$func_name _ $type_name0 N _ $type_name1 N _ $type_name2>](
                arg0: Option<$arg_type0>,
                arg1: Option<$arg_type1>,
                arg2: $arg_type2
            ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg1 = arg1?;
                Some([<$func_name _ $type_name0 _ $type_name1 _ $type_name2>](arg0, arg1, arg2))
            }

            #[doc(hidden)]
            pub fn [<$func_name _ $type_name0 N _ $type_name1 N _ $type_name2 N>](
                arg0: Option<$arg_type0>,
                arg1: Option<$arg_type1>,
                arg2: Option<$arg_type2>
            ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg1 = arg1?;
                let arg2 = arg2?;
                Some([<$func_name _ $type_name0 _ $type_name1 _ $type_name2>](arg0, arg1, arg2))
            }
        }
    };
}

pub(crate) use some_polymorphic_function3;

// If there exists a function is f___(x: T, y: S, z: V) -> U, this creates
// seven functions:
// - f__N(x: T, y: S, z: Option<V>) -> Option<U>
// - f_N_(x: T, y: Option<S>, z: V) -> Option<U>
// - etc.
// The resulting functions return Some only if all arguments are 'Some'.
macro_rules! some_function3 {
    ($func_name:ident, $arg_type0:ty, $arg_type1:ty, $arg_type2: ty, $ret_type:ty) => {
        ::paste::paste! {
            #[doc(hidden)]
            pub fn [<$func_name __N>]( arg0: $arg_type0, arg1: $arg_type1, arg2: Option<$arg_type2> ) -> Option<$ret_type> {
                let arg2 = arg2?;
                Some([<$func_name ___>](arg0, arg1, arg2))
            }

            #[doc(hidden)]
            pub fn [<$func_name _N_>]( arg0: $arg_type0, arg1: Option<$arg_type1>, arg2: $arg_type2 ) -> Option<$ret_type> {
                let arg1 = arg1?;
                Some([<$func_name ___>](arg0, arg1, arg2))
            }

            #[doc(hidden)]
            pub fn [<$func_name _NN>]( arg0: $arg_type0, arg1: Option<$arg_type1>, arg2: Option<$arg_type2> ) -> Option<$ret_type> {
                let arg1 = arg1?;
                let arg2 = arg2?;
                Some([<$func_name ___>](arg0, arg1, arg2))
            }

            #[doc(hidden)]
            pub fn [<$func_name N__>]( arg0: Option<$arg_type0>, arg1: $arg_type1, arg2: $arg_type2 ) -> Option<$ret_type> {
                let arg0 = arg0?;
                Some([<$func_name ___>](arg0, arg1, arg2))
            }

            #[doc(hidden)]
            pub fn [<$func_name N_N>]( arg0: Option<$arg_type0>, arg1: $arg_type1, arg2: Option<$arg_type2> ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg2 = arg2?;
                Some([<$func_name ___>](arg0, arg1, arg2))
            }

            #[doc(hidden)]
            pub fn [<$func_name NN_>]( arg0: Option<$arg_type0>, arg1: Option<$arg_type1>, arg2: $arg_type2 ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg1 = arg1?;
                Some([<$func_name ___>](arg0, arg1, arg2))
            }

            #[doc(hidden)]
            pub fn [<$func_name NNN>]( arg0: Option<$arg_type0>, arg1: Option<$arg_type1>, arg2: Option<$arg_type2> ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg1 = arg1?;
                let arg2 = arg2?;
                Some([<$func_name ___>](arg0, arg1, arg2))
            }
        }
    }
}

pub(crate) use some_function3;

// Macro to create variants of a function with 4 arguments
// If there exists a function is f____(x: T, y: S, z: V, w: W) -> U, this
// creates fifteen functions:
// - f___N(x: T, y: S, z: V, w: Option<W>) -> Option<U>
// - f__N_(x: T, y: S, z: Option<V>, w: W) -> Option<U>
// - etc.
// The resulting functions return Some only if all arguments are 'Some'.
macro_rules! some_function4 {
    ($func_name:ident, $arg_type0:ty, $arg_type1:ty, $arg_type2: ty, $arg_type3: ty, $ret_type:ty) => {
        ::paste::paste! {
            #[doc(hidden)]
            pub fn [<$func_name ___N>]( arg0: $arg_type0, arg1: $arg_type1, arg2: $arg_type2, arg3: Option<$arg_type3> ) -> Option<$ret_type> {
                let arg3 = arg3?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            #[doc(hidden)]
            pub fn [<$func_name __N_>]( arg0: $arg_type0, arg1: $arg_type1, arg2: Option<$arg_type2>, arg3: $arg_type3 ) -> Option<$ret_type> {
                let arg2 = arg2?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            #[doc(hidden)]
            pub fn [<$func_name __NN>]( arg0: $arg_type0, arg1: $arg_type1, arg2: Option<$arg_type2>, arg3: Option<$arg_type3> ) -> Option<$ret_type> {
                let arg2 = arg2?;
                let arg3 = arg3?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            #[doc(hidden)]
            pub fn [<$func_name _N__>]( arg0: $arg_type0, arg1: Option<$arg_type1>, arg2: $arg_type2, arg3: $arg_type3 ) -> Option<$ret_type> {
                let arg1 = arg1?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            #[doc(hidden)]
            pub fn [<$func_name _N_N>]( arg0: $arg_type0, arg1: Option<$arg_type1>, arg2: $arg_type2, arg3: Option<$arg_type3> ) -> Option<$ret_type> {
                let arg1 = arg1?;
                let arg3 = arg3?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            #[doc(hidden)]
            pub fn [<$func_name _NN_>]( arg0: $arg_type0, arg1: Option<$arg_type1>, arg2: Option<$arg_type2>, arg3: $arg_type3 ) -> Option<$ret_type> {
                let arg1 = arg1?;
                let arg2 = arg2?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            #[doc(hidden)]
            pub fn [<$func_name _NNN>]( arg0: $arg_type0, arg1: Option<$arg_type1>, arg2: Option<$arg_type2>, arg3: Option<$arg_type3> ) -> Option<$ret_type> {
                let arg1 = arg1?;
                let arg2 = arg2?;
                let arg3 = arg3?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            #[doc(hidden)]
            pub fn [<$func_name N___>]( arg0: Option<$arg_type0>, arg1: $arg_type1, arg2: $arg_type2, arg3: $arg_type3 ) -> Option<$ret_type> {
                let arg0 = arg0?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            #[doc(hidden)]
            pub fn [<$func_name N__N>]( arg0: Option<$arg_type0>, arg1: $arg_type1, arg2: Option<$arg_type2>, arg3: Option<$arg_type3> ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg2 = arg2?;
                let arg3 = arg3?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            #[doc(hidden)]
            pub fn [<$func_name N_N_>]( arg0: Option<$arg_type0>, arg1: Option<$arg_type1>, arg2: $arg_type2, arg3: $arg_type3 ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg1 = arg1?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            #[doc(hidden)]
            pub fn [<$func_name N_NN>]( arg0: Option<$arg_type0>, arg1: Option<$arg_type1>, arg2: Option<$arg_type2>, arg3: Option<$arg_type3> ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg1 = arg1?;
                let arg2 = arg2?;
                let arg3 = arg3?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            #[doc(hidden)]
            pub fn [<$func_name NN__>]( arg0: Option<$arg_type0>, arg1: $arg_type1, arg2: $arg_type2, arg3: $arg_type3 ) -> Option<$ret_type> {
                let arg0 = arg0?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            #[doc(hidden)]
            pub fn [<$func_name NN_N>]( arg0: Option<$arg_type0>, arg1: $arg_type1, arg2: Option<$arg_type2>, arg3: Option<$arg_type3> ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg2 = arg2?;
                let arg3 = arg3?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            #[doc(hidden)]
            pub fn [<$func_name NNN_>]( arg0: Option<$arg_type0>, arg1: Option<$arg_type1>, arg2: Option<$arg_type2>, arg3: $arg_type3 ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg1 = arg1?;
                let arg2 = arg2?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            #[doc(hidden)]
            pub fn [<$func_name NNNN>]( arg0: Option<$arg_type0>, arg1: Option<$arg_type1>, arg2: Option<$arg_type2>, arg3: Option<$arg_type3> ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg1 = arg1?;
                let arg2 = arg2?;
                let arg3 = arg3?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }
        }
    }
}

pub(crate) use some_function4;

// Macro to create variants of a function with 2 arguments
// optimized for the implementation of arithmetic operators.
// Assuming there exists a function is f__(x: T, y: T) -> U, this creates
// three functions:
// - f_tN_t(x: T, y: Option<T>) -> Option<U>
// - f_t_tN(x: Option<T>, y: T) -> Option<U>
// - f_tN_tN(x: Option<T>, y: Option<T>) -> Option<U>
// The resulting functions return Some only if all arguments are 'Some'.
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

// Macro to create variants of a function with 2 arguments
// optimized for the implementation of arithmetic operators.
// Assuming there exists a function is f(x: T, y: T) -> U, this creates
// four functions:
// - f_t_t(x: T, y: T) -> U
// - f_tN_t(x: T, y: Option<T>) -> Option<U>
// - f_t_tN(x: Option<T>, y: T) -> Option<U>
// - f_tN_tN(x: Option<T>, y: Option<T>) -> Option<U>
// The resulting functions return Some only if all arguments are 'Some'.
//
// Has two variants:
// - Takes the name of the existing function, the generated functions will have
// this prefix
// - Takes the name of the existing function, and the prefix for the generated
// functions
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

macro_rules! for_all_int_operator {
    ($func_name: ident) => {
        some_operator!($func_name, i8, i8, i8);
        some_operator!($func_name, i16, i16, i16);
        some_operator!($func_name, i32, i32, i32);
        some_operator!($func_name, i64, i64, i64);
        some_operator!($func_name, i128, i128, i128);
        some_operator!($func_name, u8, u8, u8);
        some_operator!($func_name, u16, u16, u16);
        some_operator!($func_name, u32, u32, u32);
        some_operator!($func_name, u64, u64, u64);
    };
}
pub(crate) use for_all_int_operator;

#[doc(hidden)]
#[inline(always)]
pub fn wrap_bool(b: Option<bool>) -> bool {
    b.unwrap_or_default()
}

#[doc(hidden)]
#[inline(always)]
pub fn or_b_b<F>(left: bool, right: F) -> bool
where
    F: Fn() -> bool,
{
    left || right()
}

#[doc(hidden)]
#[inline(always)]
pub fn or_bN_b<F>(left: Option<bool>, right: F) -> Option<bool>
where
    F: Fn() -> bool,
{
    match left {
        Some(l) => Some(l || right()),
        None => match right() {
            true => Some(true),
            _ => None,
        },
    }
}

#[doc(hidden)]
#[inline(always)]
pub fn or_b_bN<F>(left: bool, right: F) -> Option<bool>
where
    F: Fn() -> Option<bool>,
{
    match left {
        false => right(),
        true => Some(true),
    }
}

#[doc(hidden)]
#[inline(always)]
pub fn or_bN_bN<F>(left: Option<bool>, right: F) -> Option<bool>
where
    F: Fn() -> Option<bool>,
{
    match left {
        None => match right() {
            Some(true) => Some(true),
            _ => None,
        },
        Some(false) => right(),
        Some(true) => Some(true),
    }
}

// OR and AND are special, they can't be generated by rules

#[doc(hidden)]
#[inline(always)]
pub fn and_b_b<F>(left: bool, right: F) -> bool
where
    F: Fn() -> bool,
{
    left && right()
}

#[doc(hidden)]
#[inline(always)]
pub fn and_bN_b<F>(left: Option<bool>, right: F) -> Option<bool>
where
    F: Fn() -> bool,
{
    match left {
        Some(false) => Some(false),
        Some(true) => Some(right()),
        None => match right() {
            false => Some(false),
            _ => None,
        },
    }
}

#[doc(hidden)]
#[inline(always)]
pub fn and_b_bN<F>(left: bool, right: F) -> Option<bool>
where
    F: Fn() -> Option<bool>,
{
    match left {
        false => Some(false),
        true => right(),
    }
}

#[doc(hidden)]
#[inline(always)]
pub fn and_bN_bN<F>(left: Option<bool>, right: F) -> Option<bool>
where
    F: Fn() -> Option<bool>,
{
    match left {
        Some(false) => Some(false),
        Some(true) => right(),
        None => match right() {
            Some(false) => Some(false),
            _ => None,
        },
    }
}

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

some_polymorphic_function1!(abs, i8, i8, i8);
some_polymorphic_function1!(abs, i16, i16, i16);
some_polymorphic_function1!(abs, i32, i32, i32);
some_polymorphic_function1!(abs, i64, i64, i64);
some_polymorphic_function1!(abs, i128, i128, i128);
some_polymorphic_function1!(abs, f, F32, F32);
some_polymorphic_function1!(abs, d, F64, F64);

#[doc(hidden)]
#[inline(always)]
pub const fn is_true_b_(left: bool) -> bool {
    left
}

#[doc(hidden)]
#[inline(always)]
pub const fn is_true_bN_(left: Option<bool>) -> bool {
    matches!(left, Some(true))
}

#[doc(hidden)]
#[inline(always)]
pub fn is_false_b_(left: bool) -> bool {
    !left
}

#[doc(hidden)]
#[inline(always)]
pub const fn is_false_bN_(left: Option<bool>) -> bool {
    matches!(left, Some(false))
}

#[doc(hidden)]
#[inline(always)]
pub const fn is_not_true_b_(left: bool) -> bool {
    !left
}

#[doc(hidden)]
#[inline(always)]
pub const fn is_not_true_bN_(left: Option<bool>) -> bool {
    match left {
        Some(true) => false,
        Some(false) => true,
        _ => true,
    }
}

#[doc(hidden)]
#[inline(always)]
pub const fn is_not_false_b_(left: bool) -> bool {
    left
}

#[doc(hidden)]
#[inline(always)]
pub const fn is_not_false_bN_(left: Option<bool>) -> bool {
    match left {
        Some(true) => true,
        Some(false) => false,
        _ => true,
    }
}

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
