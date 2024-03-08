#![allow(non_snake_case)]

pub mod aggregates;
pub mod array;
pub mod binary;
pub mod casts;
pub mod geopoint;
pub mod interval;
pub mod operators;
pub mod source;
pub mod string;
pub mod timestamp;

use casts::cast_to_decimal_decimal;
pub use geopoint::GeoPoint;
pub use interval::LongInterval;
pub use interval::ShortInterval;
use num_traits::Pow;
use num_traits::Zero;
pub use source::{SourcePosition, SourcePositionRange};
pub use timestamp::Date;
pub use timestamp::Time;
pub use timestamp::Timestamp;

use dbsp::algebra::{OrdIndexedZSetFactories, OrdZSetFactories};
use dbsp::dynamic::{DowncastTrait, DynData, Erase};
use dbsp::trace::ord::vec::indexed_wset_batch::VecIndexedWSetBuilder;
use dbsp::trace::ord::vec::wset_batch::VecWSetBuilder;
use dbsp::trace::BatchReaderFactories;
use dbsp::typed_batch::TypedBatch;
use dbsp::{
    algebra::{
        AddByRef, HasOne, HasZero, NegByRef, Semigroup, SemigroupValue, ZRingValue, F32, F64,
    },
    trace::{BatchReader, Builder, Cursor},
    utils::*,
    DBData, OrdIndexedZSet, OrdZSet, OutputHandle, SetHandle, ZSetHandle, ZWeight,
};
use num::{Signed, ToPrimitive};
use rust_decimal::{Decimal, MathematicalOps};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::{Add, Deref, Neg};
use std::str::FromStr;

pub type Weight = i64; // Default weight type
pub type WSet<D> = OrdZSet<D>;
pub type IndexedWSet<K, D> = OrdIndexedZSet<K, D>;

#[derive(Clone)]
pub struct DefaultOptSemigroup<T>(PhantomData<T>);

// Macro to create variants of a function with 1 argument
// If there exists a function is f_(x: T) -> S, this creates a function
// fN(x: Option<T>) -> Option<S>, defined as
// fN(x) { let x = x?; Some(f_(x)) }.
#[macro_export]
macro_rules! some_function1 {
    ($func_name:ident, $arg_type:ty, $ret_type:ty) => {
        ::paste::paste! {
            pub fn [<$func_name N>]( arg: Option<$arg_type> ) -> Option<$ret_type> {
                let arg = arg?;
                Some([<$func_name _>](arg))
            }
        }
    };
}

// Macro to create variants of a function with 1 argument
// If there exists a function is f_type(x: T) -> S, this creates a function
// f_typeN(x: Option<T>) -> Option<S>
// { let x = x?; Some(f_type(x)) }.
#[macro_export]
macro_rules! some_polymorphic_function1 {
    ($func_name:ident, $type_name: ident, $arg_type:ty, $ret_type:ty) => {
        ::paste::paste! {
            pub fn [<$func_name _ $type_name N>]( arg: Option<$arg_type> ) -> Option<$ret_type> {
                let arg = arg?;
                Some([<$func_name _ $type_name >](arg))
            }
        }
    };
}

// Macro to create variants of a polymorphic function with 1 argument that is
// also polymorphic in the return type.
// If there exists a function is f_type_result(x: T) -> S, this creates a
// function
// f_typeN_resultN(x: Option<T>) -> Option<S>
// { let x = x?; Some(f_type(x)) }.
#[macro_export]
macro_rules! polymorphic_return_function1 {
    ($func_name:ident, $type_name: ident, $arg_type:ty, $ret_name: ident, $ret_type:ty) => {
        ::paste::paste! {
            pub fn [<$func_name _ $type_name N _ $ret_name N>]( arg: Option<$arg_type> ) -> Option<$ret_type> {
                let arg = arg?;
                Some([<$func_name _ $type_name >](arg))
            }
        }
    };
}

// Macro to create variants of a function with 2 arguments
// If there exists a function is f__(x: T, y: S) -> U, this creates
// three functions:
// - f_N(x: T, y: Option<S>) -> Option<U>
// - fN_(x: Option<T>, y: S) -> Option<U>
// - fNN(x: Option<T>, y: Option<S>) -> Option<U>
// The resulting functions return Some only if all arguments are 'Some'.
#[macro_export]
macro_rules! some_function2 {
    ($func_name:ident, $arg_type0:ty, $arg_type1:ty, $ret_type:ty) => {
        ::paste::paste! {
            pub fn [<$func_name NN>]( arg0: Option<$arg_type0>, arg1: Option<$arg_type1> ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg1 = arg1?;
                Some([<$func_name __>](arg0, arg1))
            }

            pub fn [<$func_name _N>]( arg0: $arg_type0, arg1: Option<$arg_type1> ) -> Option<$ret_type> {
                let arg1 = arg1?;
                Some([<$func_name __>](arg0, arg1))
            }

            pub fn [<$func_name N_>]( arg0: Option<$arg_type0>, arg1: $arg_type1 ) -> Option<$ret_type> {
                let arg0 = arg0?;
                Some([<$func_name __>](arg0, arg1))
            }
        }
    }
}

// Macro to create variants of a function with 2 arguments and
// a generic trait bound
// If there exists a function is f__<T: TraitBound>(x: X, y: S) -> U, this
// creates three functions:
// - f_N<T: TraitBound>(x: X, y: Option<S>) -> Option<U>
// - fN_<T: TraitBound>(x: Option<X>, y: S) -> Option<U>
// - fNN<T: TraitBound>(x: Option<X>, y: Option<S>) -> Option<U>
// The resulting functions return Some only if all arguments are 'Some'.
// The generic type may be a part of the type of both parameters, just one of
// them or even the return type
#[macro_export]
macro_rules! some_generic_function2 {
    ($func_name:ident, $generic_type: ty, $arg_type0: ty, $arg_type1: ty, $trait_bound:ident, $ret_type: ty) => {
        ::paste::paste! {
            pub fn [<$func_name NN>]<$generic_type>( arg0: Option<$arg_type0>, arg1: Option<$arg_type1> ) -> Option<$ret_type>
            where
                $generic_type: $trait_bound,
            {
                let arg0 = arg0?;
                let arg1 = arg1?;
                Some([<$func_name __>](arg0, arg1))
            }

            pub fn [<$func_name _N>]<$generic_type>( arg0: $arg_type0, arg1: Option<$arg_type1> ) -> Option<$ret_type>
            where
                $generic_type: $trait_bound,
            {
                let arg1 = arg1?;
                Some([<$func_name __>](arg0, arg1))
            }

            pub fn [<$func_name N_>]<$generic_type>( arg0: Option<$arg_type0>, arg1: $arg_type1 ) -> Option<$ret_type>
            where
                $generic_type: $trait_bound,
            {
                let arg0 = arg0?;
                Some([<$func_name __>](arg0, arg1))
            }
        }
    };
}

// Macro to create variants of a polymorphic function with 2 arguments
// that is also polymorphic in the return type
// If there exists a function is f_type1_type2_result(x: T, y: S) -> U, this
// creates three functions:
// - f_type1_type2N_resultN(x: T, y: Option<S>) -> Option<U>
// - f_type1N_type2_resultN(x: Option<T>, y: S) -> Option<U>
// - f_type1N_type2N_resultN(x: Option<T>, y: Option<S>) -> Option<U>
// The resulting functions return Some only if all arguments are 'Some'.
#[macro_export]
macro_rules! polymorphic_return_function2 {
    ($func_name:ident, $type_name0: ident, $arg_type0:ty, $type_name1: ident, $arg_type1:ty, $ret_name: ident, $ret_type:ty) => {
        ::paste::paste! {
            pub fn [<$func_name _$type_name0 _ $type_name1 N _ $ret_name N>]( arg0: $arg_type0, arg1: Option<$arg_type1> ) -> Option<$ret_type> {
                let arg1 = arg1?;
                Some([<$func_name _ $type_name0 _ $type_name1 _ $ret_name>](arg0, arg1))
            }

            pub fn [<$func_name _ $type_name0 N _ $type_name1 _ $ret_name N>]( arg0: Option<$arg_type0>, arg1: $arg_type1 ) -> Option<$ret_type> {
                let arg0 = arg0?;
                Some([<$func_name _ $type_name0 _ $type_name1 _ $ret_name>](arg0, arg1))
            }

            pub fn [<$func_name _ $type_name0 N _ $type_name1 N _ $ret_name N>]( arg0: Option<$arg_type0>, arg1: Option<$arg_type1> ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg1 = arg1?;
                Some([<$func_name _ $type_name0 _ $type_name1 _ $ret_name>](arg0, arg1))
            }
        }
    }
}

// Macro to create variants of a polymorphic function with 2 arguments
// If there exists a function is f_type1_type2(x: T, y: S) -> U, this
// creates three functions:
// - f_type1_type2N(x: T, y: Option<S>) -> Option<U>
// - f_type1N_type2(x: Option<T>, y: S) -> Option<U>
// - f_type1N_type2N(x: Option<T>, y: Option<S>) -> Option<U>
// The resulting functions return Some only if all arguments are 'Some'.
#[macro_export]
macro_rules! some_polymorphic_function2 {
    ($func_name:ident, $type_name0: ident, $arg_type0:ty, $type_name1: ident, $arg_type1:ty, $ret_type:ty) => {
        ::paste::paste! {
            pub fn [<$func_name _$type_name0 _ $type_name1 N>]( arg0: $arg_type0, arg1: Option<$arg_type1> ) -> Option<$ret_type> {
                let arg1 = arg1?;
                Some([<$func_name _ $type_name0 _ $type_name1>](arg0, arg1))
            }

            pub fn [<$func_name _ $type_name0 N _ $type_name1>]( arg0: Option<$arg_type0>, arg1: $arg_type1 ) -> Option<$ret_type> {
                let arg0 = arg0?;
                Some([<$func_name _ $type_name0 _ $type_name1>](arg0, arg1))
            }

            pub fn [<$func_name _ $type_name0 N _ $type_name1 N>]( arg0: Option<$arg_type0>, arg1: Option<$arg_type1> ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg1 = arg1?;
                Some([<$func_name _ $type_name0 _ $type_name1>](arg0, arg1))
            }
        }
    }
}

// If there exists a function is f_t1_t2_t3(x: T, y: S, z: V) -> U, this creates
// seven functions:
// - f_t1_t2_t3N(x: T, y: S, z: Option<V>) -> Option<U>
// - f_t1_t2N_t2(x: T, y: Option<S>, z: V) -> Option<U>
// - etc.
// The resulting functions return Some only if all arguments are 'Some'.
#[macro_export]
macro_rules! some_polymorphic_function3 {
    ($func_name:ident,
     $type_name0: ident, $arg_type0:ty,
     $type_name1: ident, $arg_type1:ty,
     $type_name2: ident, $arg_type2: ty,
     $ret_type:ty) => {
        ::paste::paste! {
            pub fn [<$func_name _ $type_name0 _ $type_name1 _ $type_name2 N>](
                arg0: $arg_type0,
                arg1: $arg_type1,
                arg2: Option<$arg_type2>
            ) -> Option<$ret_type> {
                let arg2 = arg2?;
                Some([<$func_name _ $type_name0 _ $type_name1 _ $type_name2>](arg0, arg1, arg2))
            }

            pub fn [<$func_name _ $type_name0 _ $type_name1 N _ $type_name2>](
                arg0: $arg_type0,
                arg1: Option<$arg_type1>,
                arg2: $arg_type2
            ) -> Option<$ret_type> {
                let arg1 = arg1?;
                Some([<$func_name _ $type_name0 _ $type_name1 _ $type_name2>](arg0, arg1, arg2))
            }

            pub fn [<$func_name _ $type_name0 _ $type_name1 N _ $type_name2 N>](
                arg0: $arg_type0,
                arg1: Option<$arg_type1>,
                arg2: Option<$arg_type2>
            ) -> Option<$ret_type> {
                let arg1 = arg1?;
                let arg2 = arg2?;
                Some([<$func_name _ $type_name0 _ $type_name1 _ $type_name2>](arg0, arg1, arg2))
            }

            pub fn [<$func_name _ $type_name0 N _ $type_name1 _ $type_name2>](
                arg0: Option<$arg_type0>,
                arg1: $arg_type1,
                arg2: $arg_type2
            ) -> Option<$ret_type> {
                let arg0 = arg0?;
                Some([<$func_name _ $type_name0 _ $type_name1 _ $type_name2>](arg0, arg1, arg2))
            }

            pub fn [<$func_name _ $type_name0 N _ $type_name1 _ $type_name2 N>](
                arg0: Option<$arg_type0>,
                arg1: $arg_type1,
                arg2: Option<$arg_type2>
            ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg2 = arg2?;
                Some([<$func_name _ $type_name0 _ $type_name1 _ $type_name2>](arg0, arg1, arg2))
            }

            pub fn [<$func_name _ $type_name0 N _ $type_name1 N _ $type_name2>](
                arg0: Option<$arg_type0>,
                arg1: Option<$arg_type1>,
                arg2: $arg_type2
            ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg1 = arg1?;
                Some([<$func_name _ $type_name0 _ $type_name1 _ $type_name2>](arg0, arg1, arg2))
            }

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

// If there exists a function is f___(x: T, y: S, z: V) -> U, this creates
// seven functions:
// - f__N(x: T, y: S, z: Option<V>) -> Option<U>
// - f_N_(x: T, y: Option<S>, z: V) -> Option<U>
// - etc.
// The resulting functions return Some only if all arguments are 'Some'.
#[macro_export]
macro_rules! some_function3 {
    ($func_name:ident, $arg_type0:ty, $arg_type1:ty, $arg_type2: ty, $ret_type:ty) => {
        ::paste::paste! {
            pub fn [<$func_name __N>]( arg0: $arg_type0, arg1: $arg_type1, arg2: Option<$arg_type2> ) -> Option<$ret_type> {
                let arg2 = arg2?;
                Some([<$func_name ___>](arg0, arg1, arg2))
            }

            pub fn [<$func_name _N_>]( arg0: $arg_type0, arg1: Option<$arg_type1>, arg2: $arg_type2 ) -> Option<$ret_type> {
                let arg1 = arg1?;
                Some([<$func_name ___>](arg0, arg1, arg2))
            }

            pub fn [<$func_name _NN>]( arg0: $arg_type0, arg1: Option<$arg_type1>, arg2: Option<$arg_type2> ) -> Option<$ret_type> {
                let arg1 = arg1?;
                let arg2 = arg2?;
                Some([<$func_name ___>](arg0, arg1, arg2))
            }

            pub fn [<$func_name N__>]( arg0: Option<$arg_type0>, arg1: $arg_type1, arg2: $arg_type2 ) -> Option<$ret_type> {
                let arg0 = arg0?;
                Some([<$func_name ___>](arg0, arg1, arg2))
            }

            pub fn [<$func_name N_N>]( arg0: Option<$arg_type0>, arg1: $arg_type1, arg2: Option<$arg_type2> ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg2 = arg2?;
                Some([<$func_name ___>](arg0, arg1, arg2))
            }

            pub fn [<$func_name NN_>]( arg0: Option<$arg_type0>, arg1: Option<$arg_type1>, arg2: $arg_type2 ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg1 = arg1?;
                Some([<$func_name ___>](arg0, arg1, arg2))
            }

            pub fn [<$func_name NNN>]( arg0: Option<$arg_type0>, arg1: Option<$arg_type1>, arg2: Option<$arg_type2> ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg1 = arg1?;
                let arg2 = arg2?;
                Some([<$func_name ___>](arg0, arg1, arg2))
            }
        }
    }
}

// Macro to create variants of a function with 4 arguments
// If there exists a function is f____(x: T, y: S, z: V, w: W) -> U, this
// creates fifteen functions:
// - f___N(x: T, y: S, z: V, w: Option<W>) -> Option<U>
// - f__N_(x: T, y: S, z: Option<V>, w: W) -> Option<U>
// - etc.
// The resulting functions return Some only if all arguments are 'Some'.
#[macro_export]
macro_rules! some_function4 {
    ($func_name:ident, $arg_type0:ty, $arg_type1:ty, $arg_type2: ty, $arg_type3: ty, $ret_type:ty) => {
        ::paste::paste! {
            pub fn [<$func_name ___N>]( arg0: $arg_type0, arg1: $arg_type1, arg2: $arg_type2, arg3: Option<$arg_type3> ) -> Option<$ret_type> {
                let arg3 = arg3?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            pub fn [<$func_name __N_>]( arg0: $arg_type0, arg1: $arg_type1, arg2: Option<$arg_type2>, arg3: $arg_type3 ) -> Option<$ret_type> {
                let arg2 = arg2?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            pub fn [<$func_name __NN>]( arg0: $arg_type0, arg1: $arg_type1, arg2: Option<$arg_type2>, arg3: Option<$arg_type3> ) -> Option<$ret_type> {
                let arg2 = arg2?;
                let arg3 = arg3?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            pub fn [<$func_name _N__>]( arg0: $arg_type0, arg1: Option<$arg_type1>, arg2: $arg_type2, arg3: $arg_type3 ) -> Option<$ret_type> {
                let arg1 = arg1?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            pub fn [<$func_name _N_N>]( arg0: $arg_type0, arg1: Option<$arg_type1>, arg2: $arg_type2, arg3: Option<$arg_type3> ) -> Option<$ret_type> {
                let arg1 = arg1?;
                let arg3 = arg3?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            pub fn [<$func_name _NN_>]( arg0: $arg_type0, arg1: Option<$arg_type1>, arg2: Option<$arg_type2>, arg3: $arg_type3 ) -> Option<$ret_type> {
                let arg1 = arg1?;
                let arg2 = arg2?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            pub fn [<$func_name _NNN>]( arg0: $arg_type0, arg1: Option<$arg_type1>, arg2: Option<$arg_type2>, arg3: Option<$arg_type3> ) -> Option<$ret_type> {
                let arg1 = arg1?;
                let arg2 = arg2?;
                let arg3 = arg3?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            pub fn [<$func_name N___>]( arg0: Option<$arg_type0>, arg1: $arg_type1, arg2: $arg_type2, arg3: $arg_type3 ) -> Option<$ret_type> {
                let arg0 = arg0?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            pub fn [<$func_name N__N>]( arg0: Option<$arg_type0>, arg1: $arg_type1, arg2: Option<$arg_type2>, arg3: Option<$arg_type3> ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg2 = arg2?;
                let arg3 = arg3?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            pub fn [<$func_name N_N_>]( arg0: Option<$arg_type0>, arg1: Option<$arg_type1>, arg2: $arg_type2, arg3: $arg_type3 ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg1 = arg1?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            pub fn [<$func_name N_NN>]( arg0: Option<$arg_type0>, arg1: Option<$arg_type1>, arg2: Option<$arg_type2>, arg3: Option<$arg_type3> ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg1 = arg1?;
                let arg2 = arg2?;
                let arg3 = arg3?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            pub fn [<$func_name NN__>]( arg0: Option<$arg_type0>, arg1: $arg_type1, arg2: $arg_type2, arg3: $arg_type3 ) -> Option<$ret_type> {
                let arg0 = arg0?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            pub fn [<$func_name NN_N>]( arg0: Option<$arg_type0>, arg1: $arg_type1, arg2: Option<$arg_type2>, arg3: Option<$arg_type3> ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg2 = arg2?;
                let arg3 = arg3?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

            pub fn [<$func_name NNN_>]( arg0: Option<$arg_type0>, arg1: Option<$arg_type1>, arg2: Option<$arg_type2>, arg3: $arg_type3 ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg1 = arg1?;
                let arg2 = arg2?;
                Some([<$func_name ____>](arg0, arg1, arg2, arg3))
            }

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

// Macro to create variants of a function with 2 arguments
// optimized for the implementation of arithmetic operators.
// Assuming there exists a function is f__(x: T, y: T) -> U, this creates
// three functions:
// - f_tN_t(x: T, y: Option<T>) -> Option<U>
// - f_t_tN(x: Option<T>, y: T) -> Option<U>
// - f_tN_tN(x: Option<T>, y: Option<T>) -> Option<U>
// The resulting functions return Some only if all arguments are 'Some'.
#[macro_export]
macro_rules! some_existing_operator {
    ($func_name: ident, $short_name: ident, $arg_type: ty, $ret_type: ty) => {
        ::paste::paste! {
            #[inline(always)]
            pub fn [<$func_name _ $short_name N _ $short_name N>]( arg0: Option<$arg_type>, arg1: Option<$arg_type> ) -> Option<$ret_type> {
                let arg0 = arg0?;
                let arg1 = arg1?;
                Some([<$func_name _ $short_name _ $short_name>](arg0, arg1))
            }

            #[inline(always)]
            pub fn [<$func_name _ $short_name _ $short_name N>]( arg0: $arg_type, arg1: Option<$arg_type> ) -> Option<$ret_type> {
                let arg1 = arg1?;
                Some([<$func_name _ $short_name _ $short_name>](arg0, arg1))
            }

            #[inline(always)]
            pub fn [<$func_name _ $short_name N _ $short_name>]( arg0: Option<$arg_type>, arg1: $arg_type ) -> Option<$ret_type> {
                let arg0 = arg0?;
                Some([<$func_name _ $short_name _ $short_name>](arg0, arg1))
            }
        }
    }
}

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
#[macro_export]
macro_rules! some_operator {
    ($func_name: ident, $short_name: ident, $arg_type: ty, $ret_type: ty) => {
        ::paste::paste! {
            #[inline(always)]
            pub fn [<$func_name _ $short_name _ $short_name >]( arg0: $arg_type, arg1: $arg_type ) -> $ret_type {
                $func_name(arg0, arg1)
            }

            some_existing_operator!($func_name, $short_name, $arg_type, $ret_type);
        }
    };
    ($func_name: ident, $new_func_name: ident, $short_name: ident, $arg_type: ty, $ret_type: ty) => {
        ::paste::paste! {
            #[inline(always)]
            pub fn [<$new_func_name _ $short_name _ $short_name >]( arg0: $arg_type, arg1: $arg_type ) -> $ret_type {
                $func_name(arg0, arg1)
            }

            some_existing_operator!($new_func_name, $short_name, $arg_type, $ret_type);
        }
    }
}

#[macro_export]
macro_rules! for_all_int_compare {
    ($func_name: ident, $ret_type: ty) => {
        some_operator!($func_name, i8, i8, bool);
        some_operator!($func_name, i16, i16, bool);
        some_operator!($func_name, i32, i32, bool);
        some_operator!($func_name, i64, i64, bool);
    };
}

#[macro_export]
macro_rules! for_all_numeric_compare {
    ($func_name: ident, $ret_type: ty) => {
        for_all_int_compare!($func_name, bool);
        some_operator!($func_name, f, F32, bool);
        some_operator!($func_name, d, F64, bool);
        some_operator!($func_name, decimal, Decimal, bool);
    };
}

#[macro_export]
macro_rules! for_all_compare {
    ($func_name: ident, $ret_type: ty) => {
        for_all_numeric_compare!($func_name, bool);
        some_operator!($func_name, b, bool, bool);
        some_operator!($func_name, s, String, bool);
    };
}

#[macro_export]
macro_rules! for_all_int_operator {
    ($func_name: ident) => {
        some_operator!($func_name, i8, i8, i8);
        some_operator!($func_name, i16, i16, i16);
        some_operator!($func_name, i32, i32, i32);
        some_operator!($func_name, i64, i64, i64);
    };
}

#[macro_export]
macro_rules! for_all_numeric_operator {
    ($func_name: ident) => {
        for_all_int_operator!($func_name);
        some_operator!($func_name, f, F32, F32);
        some_operator!($func_name, d, F64, F64);
        some_operator!($func_name, decimal, Decimal, Decimal);
    };
}

impl<T> Semigroup<Option<T>> for DefaultOptSemigroup<T>
where
    T: SemigroupValue,
{
    fn combine(left: &Option<T>, right: &Option<T>) -> Option<T> {
        match (left, right) {
            (None, _) => None,
            (_, None) => None,
            (Some(x), Some(y)) => Some(x.add_by_ref(y)),
        }
    }
}

#[derive(Clone)]
pub struct PairSemigroup<T, R, TS, RS>(PhantomData<(T, R, TS, RS)>);

impl<T, R, TS, RS> Semigroup<Tup2<T, R>> for PairSemigroup<T, R, TS, RS>
where
    TS: Semigroup<T>,
    RS: Semigroup<R>,
{
    fn combine(left: &Tup2<T, R>, right: &Tup2<T, R>) -> Tup2<T, R> {
        Tup2::new(
            TS::combine(&left.0, &right.0),
            RS::combine(&left.1, &right.1),
        )
    }
}

#[inline(always)]
pub fn wrap_bool(b: Option<bool>) -> bool {
    match b {
        Some(x) => x,
        _ => false,
    }
}

#[inline(always)]
pub fn or_b_b(left: bool, right: bool) -> bool {
    left || right
}

#[inline(always)]
pub fn or_bN_b(left: Option<bool>, right: bool) -> Option<bool> {
    match (left, right) {
        (Some(l), r) => Some(l || r),
        (_, true) => Some(true),
        (_, _) => None::<bool>,
    }
}

#[inline(always)]
pub fn or_b_bN(left: bool, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (l, Some(r)) => Some(l || r),
        (true, _) => Some(true),
        (_, _) => None::<bool>,
    }
}

#[inline(always)]
pub fn or_bN_bN(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(l), Some(r)) => Some(l || r),
        (Some(true), _) => Some(true),
        (_, Some(true)) => Some(true),
        (_, _) => None::<bool>,
    }
}

// OR and AND are special, they can't be generated by rules

#[inline(always)]
pub fn and_b_b(left: bool, right: bool) -> bool {
    left && right
}

#[inline(always)]
pub fn and_bN_b(left: Option<bool>, right: bool) -> Option<bool> {
    match (left, right) {
        (Some(l), r) => Some(l && r),
        (_, false) => Some(false),
        (_, _) => None::<bool>,
    }
}

#[inline(always)]
pub fn and_b_bN(left: bool, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (l, Some(r)) => Some(l && r),
        (false, _) => Some(false),
        (_, _) => None::<bool>,
    }
}

#[inline(always)]
pub fn and_bN_bN(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(l), Some(r)) => Some(l && r),
        (Some(false), _) => Some(false),
        (_, Some(false)) => Some(false),
        (_, _) => None::<bool>,
    }
}

#[inline(always)]
pub fn is_null<T>(value: Option<T>) -> bool {
    value.is_none()
}

#[inline(always)]
pub fn indicator<T>(value: &Option<T>) -> i64 {
    match value {
        None => 0,
        Some(_) => 1,
    }
}

//////////////////////////////////////////

#[inline(always)]
pub fn abs_i8(left: i8) -> i8 {
    left.abs()
}

#[inline(always)]
pub fn abs_i16(left: i16) -> i16 {
    left.abs()
}

#[inline(always)]
pub fn abs_i32(left: i32) -> i32 {
    left.abs()
}

#[inline(always)]
pub fn abs_i64(left: i64) -> i64 {
    left.abs()
}

#[inline(always)]
pub fn abs_f(left: F32) -> F32 {
    left.abs()
}

#[inline(always)]
pub fn abs_d(left: F64) -> F64 {
    left.abs()
}

#[inline(always)]
pub fn abs_decimal(left: Decimal) -> Decimal {
    left.abs()
}

some_polymorphic_function1!(abs, i8, i8, i8);
some_polymorphic_function1!(abs, i16, i16, i16);
some_polymorphic_function1!(abs, i32, i32, i32);
some_polymorphic_function1!(abs, i64, i64, i64);
some_polymorphic_function1!(abs, f, F32, F32);
some_polymorphic_function1!(abs, d, F64, F64);
some_polymorphic_function1!(abs, decimal, Decimal, Decimal);

#[inline(always)]
pub fn exp_d(value: F64) -> F64 {
    value.into_inner().exp().into()
}

some_polymorphic_function1!(exp, d, F64, F64);

#[inline(always)]
pub fn ln_d(left: F64) -> F64 {
    let left = left.into_inner();

    if left.is_sign_negative() {
        panic!("Unable to calculate ln for {left}");
    }

    left.ln().into()
}

some_polymorphic_function1!(ln, d, F64, F64);

#[inline(always)]
pub fn log10_d(left: F64) -> F64 {
    let left = left.into_inner();

    if left.is_sign_negative() {
        panic!("Unable to calculate log10 for {left}");
    }

    left.log10().into()
}

some_polymorphic_function1!(log10, d, F64, F64);

#[inline(always)]
pub fn log_d(left: F64) -> F64 {
    ln_d(left)
}

some_polymorphic_function1!(log, d, F64, F64);

#[inline(always)]
pub fn log_d_d(left: F64, right: F64) -> F64 {
    let left = left.into_inner();
    let right = right.into_inner();

    if left.is_sign_negative() || right.is_sign_negative() {
        panic!("Unable to calculate log({left}, {right})")
    }

    // match Calcite's behavior, return 0 instead of -0
    if right.is_zero() {
        return F64::new(0.0);
    }

    left.log(right).into()
}

some_polymorphic_function2!(log, d, F64, d, F64, F64);

#[inline(always)]
pub fn is_true_b_(left: bool) -> bool {
    left
}

#[inline(always)]
pub fn is_true_bN_(left: Option<bool>) -> bool {
    left == Some(true)
}

#[inline(always)]
pub fn is_false_b_(left: bool) -> bool {
    !left
}

#[inline(always)]
pub fn is_false_bN_(left: Option<bool>) -> bool {
    left == Some(false)
}

#[inline(always)]
pub fn is_not_true_b_(left: bool) -> bool {
    !left
}

#[inline(always)]
pub fn is_not_true_bN_(left: Option<bool>) -> bool {
    match left {
        Some(true) => false,
        Some(false) => true,
        _ => true,
    }
}

#[inline(always)]
pub fn is_not_false_b_(left: bool) -> bool {
    left
}

#[inline(always)]
pub fn is_not_false_bN_(left: Option<bool>) -> bool {
    match left {
        Some(true) => true,
        Some(false) => false,
        _ => true,
    }
}

#[inline(always)]
pub fn is_distinct__<T>(left: T, right: T) -> bool
where
    T: Eq,
{
    left != right
}

#[inline(always)]
pub fn is_distinct_N_N<T>(left: Option<T>, right: Option<T>) -> bool
where
    T: Eq,
{
    match (left, right) {
        (Some(a), Some(b)) => a != b,
        (None, None) => false,
        _ => true,
    }
}

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

pub fn times_ShortInterval_i64(left: ShortInterval, right: i64) -> ShortInterval {
    left * right
}

some_polymorphic_function2!(times, ShortInterval, ShortInterval, i64, i64, ShortInterval);

pub fn times_i64_ShortInterval(left: i64, right: ShortInterval) -> ShortInterval {
    right * left
}

some_polymorphic_function2!(times, i64, i64, ShortInterval, ShortInterval, ShortInterval);

pub fn times_ShortInterval_i32(left: ShortInterval, right: i32) -> ShortInterval {
    left * (right as i64)
}

some_polymorphic_function2!(times, ShortInterval, ShortInterval, i32, i32, ShortInterval);

pub fn times_i32_ShortInterval(left: i32, right: ShortInterval) -> ShortInterval {
    right * (left as i64)
}

some_polymorphic_function2!(times, i32, i32, ShortInterval, ShortInterval, ShortInterval);

/***** decimals ***** */

#[inline(always)]
pub fn new_decimal(s: &str, precision: u32, scale: u32) -> Option<Decimal> {
    let value = Decimal::from_str(s).ok()?;
    Some(cast_to_decimal_decimal(value, precision, scale))
}

#[inline(always)]
pub fn round_decimal(left: Decimal, right: i32) -> Decimal {
    // Rust decimal doesn't support rounding with negative values
    // but Calcite does
    if right.is_negative() {
        let right_unsigned = right.unsigned_abs();
        let pow_of_ten = Decimal::new(10_i64.pow(right_unsigned), 0);
        let rounded = ((left / pow_of_ten).round()) * pow_of_ten;
        return rounded;
    }

    left.round_dp(u32::try_from(right).unwrap())
}

#[inline(always)]
pub fn round_decimalN(left: Option<Decimal>, right: i32) -> Option<Decimal> {
    left.map(|x| round_decimal(x, right))
}

#[inline(always)]
pub fn truncate_decimal(left: Decimal, right: i32) -> Decimal {
    // Rust decimal doesn't support rounding with negative values
    // but Calcite does
    if right.is_negative() {
        let right_unsigned = right.unsigned_abs();
        let pow_of_ten = Decimal::new(10_i64.pow(right_unsigned), 0);
        let truncated = (left / pow_of_ten).trunc() * pow_of_ten;
        return truncated;
    }

    left.trunc_with_scale(u32::try_from(right).unwrap())
}

#[inline(always)]
pub fn truncate_decimalN(left: Option<Decimal>, right: i32) -> Option<Decimal> {
    left.map(|x| truncate_decimal(x, right))
}

#[inline(always)]
pub fn truncate_d(left: F64, right: i32) -> F64 {
    let mut left = left.into_inner() * 10.0_f64.pow(right);
    left = left.trunc();
    left /= 10.0_f64.pow(right);

    if left.is_zero() {
        // normalize the sign to match Calcite
        left = 0.0;
    }
    (left).into()
}

pub fn truncate_dN(left: Option<F64>, right: i32) -> Option<F64> {
    Some(truncate_d(left?, right))
}

#[inline(always)]
pub fn round_d(left: F64, right: i32) -> F64 {
    let mut left = left.into_inner() * 10.0_f64.pow(right);
    left = left.round();
    left /= 10.0_f64.pow(right);

    if left.is_zero() {
        // normalize the sign to match Calcite
        left = 0.0;
    }

    (left).into()
}

pub fn round_dN(left: Option<F64>, right: i32) -> Option<F64> {
    Some(round_d(left?, right))
}

pub fn power_i32_d(left: i32, right: F64) -> F64 {
    F64::new((left as f64).powf(right.into_inner()))
}

some_polymorphic_function2!(power, i32, i32, d, F64, F64);

pub fn power_d_i32(left: F64, right: i32) -> F64 {
    F64::new(left.into_inner().powi(right))
}

some_polymorphic_function2!(power, d, F64, i32, i32, F64);

pub fn power_i32_decimal(left: i32, right: Decimal) -> F64 {
    F64::new((left as f64).powf(right.to_f64().unwrap()))
}

some_polymorphic_function2!(power, i32, i32, decimal, Decimal, F64);

pub fn power_decimal_i32(left: Decimal, right: i32) -> F64 {
    F64::new(left.powi(right.into()).to_f64().unwrap())
}

some_polymorphic_function2!(power, decimal, Decimal, i32, i32, F64);

pub fn power_i32_i32(left: i32, right: i32) -> F64 {
    (left as f64).pow(right).into()
}

some_polymorphic_function2!(power, i32, i32, i32, i32, F64);

pub fn power_d_d(left: F64, right: F64) -> F64 {
    F64::new(left.into_inner().powf(right.into_inner()))
}

some_polymorphic_function2!(power, d, F64, d, F64, F64);

pub fn power_decimal_decimal(left: Decimal, right: Decimal) -> F64 {
    if right == Decimal::new(5, 1) {
        // special case for sqrt, has higher precision than pow
        F64::from(left.sqrt().unwrap().to_f64().unwrap())
    } else {
        F64::from(left.powd(right).to_f64().unwrap())
    }
}

some_polymorphic_function2!(power, decimal, Decimal, decimal, Decimal, F64);

pub fn power_decimal_d(left: Decimal, right: F64) -> F64 {
    F64::new(left.powf(right.into_inner()).to_f64().unwrap())
}

some_polymorphic_function2!(power, decimal, Decimal, d, F64, F64);

pub fn power_d_decimal(left: F64, right: Decimal) -> F64 {
    F64::new(left.into_inner().powf(right.to_f64().unwrap()))
}

some_polymorphic_function2!(power, d, F64, decimal, Decimal, F64);

pub fn sqrt_decimal(left: Decimal) -> F64 {
    if left < Decimal::ZERO {
        panic!("Unable to compute sqrt of {left}");
    }

    F64::from(left.sqrt().unwrap().to_f64().unwrap())
}

some_polymorphic_function1!(sqrt, decimal, Decimal, F64);

pub fn sqrt_d(left: F64) -> F64 {
    let left = left.into_inner();
    if left < 0.0 {
        panic!("Unable to compute sqrt of {left}");
    }
    F64::new(left.sqrt())
}

some_polymorphic_function1!(sqrt, d, F64, F64);

//////////////////// floor /////////////////////

#[inline(always)]
pub fn floor_d(value: F64) -> F64 {
    F64::new(value.into_inner().floor())
}

#[inline(always)]
pub fn floor_f(value: F32) -> F32 {
    F32::new(value.into_inner().floor())
}

#[inline(always)]
pub fn floor_decimal(value: Decimal) -> Decimal {
    value.floor()
}

some_polymorphic_function1!(floor, f, F32, F32);
some_polymorphic_function1!(floor, d, F64, F64);
some_polymorphic_function1!(floor, decimal, Decimal, Decimal);

//////////////////// ceil /////////////////////

#[inline(always)]
pub fn ceil_d(value: F64) -> F64 {
    F64::new(value.into_inner().ceil())
}

#[inline(always)]
pub fn ceil_f(value: F32) -> F32 {
    F32::new(value.into_inner().ceil())
}

#[inline(always)]
pub fn ceil_decimal(value: Decimal) -> Decimal {
    value.ceil()
}

some_polymorphic_function1!(ceil, f, F32, F32);
some_polymorphic_function1!(ceil, d, F64, F64);
some_polymorphic_function1!(ceil, decimal, Decimal, Decimal);

///////////////////// sign //////////////////////

#[inline(always)]
pub fn sign_d(value: F64) -> F64 {
    // Rust signum never returns 0
    let x = value.into_inner();
    if x == 0f64 {
        value
    } else {
        F64::new(x.signum())
    }
}

#[inline(always)]
pub fn sign_f(value: F32) -> F32 {
    // Rust signum never returns 0
    let x = value.into_inner();
    if x == 0f32 {
        value
    } else {
        F32::new(x.signum())
    }
}

#[inline(always)]
pub fn sign_decimal(value: Decimal) -> Decimal {
    value.signum()
}

some_polymorphic_function1!(sign, f, F32, F32);
some_polymorphic_function1!(sign, d, F64, F64);
some_polymorphic_function1!(sign, decimal, Decimal, Decimal);

// PI
#[inline(always)]
pub fn pi() -> F64 {
    std::f64::consts::PI.into()
}

/////////// Trigonometric Fucntions //////////////

#[inline(always)]
pub fn sin_d(value: F64) -> F64 {
    value.into_inner().sin().into()
}

some_polymorphic_function1!(sin, d, F64, F64);

#[inline(always)]
pub fn cos_d(value: F64) -> F64 {
    value.into_inner().cos().into()
}

some_polymorphic_function1!(cos, d, F64, F64);

#[inline(always)]
pub fn tan_d(value: F64) -> F64 {
    value.into_inner().tan().into()
}

some_polymorphic_function1!(tan, d, F64, F64);

#[inline(always)]
pub fn sec_d(value: F64) -> F64 {
    (1.0 / value.into_inner().cos()).into()
}

some_polymorphic_function1!(sec, d, F64, F64);

#[inline(always)]
pub fn csc_d(value: F64) -> F64 {
    (1.0 / value.into_inner().sin()).into()
}

some_polymorphic_function1!(csc, d, F64, F64);

#[inline(always)]
pub fn cot_d(value: F64) -> F64 {
    (1.0_f64 / value.into_inner().tan()).into()
}

some_polymorphic_function1!(cot, d, F64, F64);

#[inline(always)]
pub fn asin_d(value: F64) -> F64 {
    value.into_inner().asin().into()
}

some_polymorphic_function1!(asin, d, F64, F64);

#[inline(always)]
pub fn acos_d(value: F64) -> F64 {
    value.into_inner().acos().into()
}

some_polymorphic_function1!(acos, d, F64, F64);

#[inline(always)]
pub fn atan_d(value: F64) -> F64 {
    value.into_inner().atan().into()
}

some_polymorphic_function1!(atan, d, F64, F64);

#[inline(always)]
pub fn atan2_d_d(y: F64, x: F64) -> F64 {
    let y = y.into_inner();
    let x = x.into_inner();

    y.atan2(x).into()
}

some_polymorphic_function2!(atan2, d, F64, d, F64, F64);

#[inline(always)]
pub fn degrees_d(value: F64) -> F64 {
    value.into_inner().to_degrees().into()
}

some_polymorphic_function1!(degrees, d, F64, F64);

#[inline(always)]
pub fn radians_d(value: F64) -> F64 {
    value.into_inner().to_radians().into()
}

some_polymorphic_function1!(radians, d, F64, F64);

#[inline(always)]
pub fn cbrt_d(value: F64) -> F64 {
    value.into_inner().cbrt().into()
}

some_polymorphic_function1!(cbrt, d, F64, F64);

/////////// Hyperbolic Fucntions //////////////

#[inline(always)]
pub fn sinh_d(value: F64) -> F64 {
    value.into_inner().sinh().into()
}

some_polymorphic_function1!(sinh, d, F64, F64);

#[inline(always)]
pub fn cosh_d(value: F64) -> F64 {
    value.into_inner().cosh().into()
}

some_polymorphic_function1!(cosh, d, F64, F64);

#[inline(always)]
pub fn tanh_d(value: F64) -> F64 {
    value.into_inner().tanh().into()
}

some_polymorphic_function1!(tanh, d, F64, F64);

#[inline(always)]
pub fn coth_d(value: F64) -> F64 {
    (1.0 / value.into_inner().tanh()).into()
}

some_polymorphic_function1!(coth, d, F64, F64);

#[inline(always)]
pub fn asinh_d(value: F64) -> F64 {
    value.into_inner().asinh().into()
}

some_polymorphic_function1!(asinh, d, F64, F64);

#[inline(always)]
pub fn acosh_d(value: F64) -> F64 {
    if value.into_inner() < 1.0 {
        panic!("input ({}) out of range [1, Infinity]", value)
    }

    value.into_inner().acosh().into()
}

some_polymorphic_function1!(acosh, d, F64, F64);

#[inline(always)]
pub fn atanh_d(value: F64) -> F64 {
    let inner = value.into_inner();
    if !(-1.0..=1.0).contains(&inner) && !inner.is_nan() {
        panic!("input ({}) out of range [-1, 1]", value)
    }

    inner.atanh().into()
}

some_polymorphic_function1!(atanh, d, F64, F64);

#[inline(always)]
pub fn csch_d(value: F64) -> F64 {
    (1.0 / value.into_inner().sinh()).into()
}

some_polymorphic_function1!(csch, d, F64, F64);

#[inline(always)]
pub fn sech_d(value: F64) -> F64 {
    (1.0 / value.into_inner().cosh()).into()
}

some_polymorphic_function1!(sech, d, F64, F64);

#[inline(always)]
pub fn is_inf_d(value: F64) -> bool {
    value.into_inner().is_infinite()
}

#[inline(always)]
pub fn is_inf_f(value: F32) -> bool {
    value.into_inner().is_infinite()
}

some_polymorphic_function1!(is_inf, d, F64, bool);
some_polymorphic_function1!(is_inf, f, F32, bool);

#[inline(always)]
pub fn is_nan_d(value: F64) -> bool {
    value.into_inner().is_nan()
}

#[inline(always)]
pub fn is_nan_f(value: F32) -> bool {
    value.into_inner().is_nan()
}

some_polymorphic_function1!(is_nan, d, F64, bool);
some_polymorphic_function1!(is_nan, f, F32, bool);

////////////////////////////////////////////////

pub fn dump<T>(prefix: String, data: &T) -> T
where
    T: Debug + Clone,
{
    println!("{}: {:?}", prefix, data);
    data.clone()
}

pub fn print(str: String) {
    print!("{}", str)
}

pub fn print_opt(str: Option<String>) {
    match str {
        None => print!("NULL"),
        Some(x) => print!("{}", x),
    }
}

pub fn zset_map<D, T, F>(data: &WSet<D>, mapper: F) -> WSet<T>
where
    D: DBData + 'static,
    T: DBData + 'static,
    F: Fn(&D) -> T,
{
    let factories = OrdZSetFactories::new::<T, (), ZWeight>();
    let mut builder = VecWSetBuilder::with_capacity(&factories, (), data.len());
    let mut cursor = data.cursor();
    while cursor.key_valid() {
        let mut weight = *cursor.weight().deref();
        let item = unsafe { cursor.key().downcast::<D>() };
        let mut data = mapper(item);
        builder.push_vals(data.erase_mut(), ().erase_mut(), weight.erase_mut());
        cursor.step_key();
    }
    TypedBatch::new(builder.done())
}

pub fn zset_filter_comparator<D, T, F>(data: &WSet<D>, value: &T, comparator: F) -> WSet<D>
where
    D: DBData + 'static,
    T: 'static,
    F: Fn(&D, &T) -> bool,
{
    let factories = OrdZSetFactories::new::<D, (), ZWeight>();

    let mut builder = VecWSetBuilder::with_capacity(&factories, (), data.len());

    let mut cursor = data.cursor();
    while cursor.key_valid() {
        let weight = *cursor.weight().deref();
        let item = unsafe { cursor.key().downcast::<D>() };
        if comparator(item, value) {
            builder.push_refs(item.erase(), ().erase(), weight.erase());
        }
        cursor.step_key();
    }
    TypedBatch::new(builder.done())
}

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
    let mut builder = VecIndexedWSetBuilder::with_capacity(&factories, (), data.len());

    let mut cursor = data.cursor();
    while cursor.key_valid() {
        let key = unsafe { cursor.key().downcast::<K>() }.clone();
        while cursor.val_valid() {
            let w = *cursor.weight().deref();
            let item = unsafe { cursor.val().downcast::<D>() };
            if comparator((&key, item), value) {
                builder.push_refs(&key, item.erase(), w.erase());
            }
            cursor.step_val();
        }
        cursor.step_key();
    }
    TypedBatch::new(builder.done())
}

pub fn append_to_upsert_handle<K>(data: &WSet<K>, handle: &SetHandle<K>)
where
    K: DBData,
{
    let mut cursor = data.cursor();
    while cursor.key_valid() {
        let mut w = *cursor.weight().deref();
        let mut insert = true;
        if !w.ge0() {
            insert = false;
            w = w.neg();
        }
        while !w.le0() {
            let key = unsafe { cursor.key().downcast::<K>() };
            handle.push(key.clone(), insert);
            w = w.add(Weight::neg(Weight::one()));
        }
        cursor.step_key();
    }
}

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

pub fn read_output_handle<K>(handle: &OutputHandle<WSet<K>>) -> WSet<K>
where
    K: DBData,
{
    handle.consolidate()
}

// Check that two zsets are equal.  If yes, returns true.
// If not, print a diff of the zsets and returns false.
// Assumes that the zsets are positive (all weights are positive).
pub fn must_equal<K>(left: &WSet<K>, right: &WSet<K>) -> bool
where
    K: DBData + Clone,
{
    let diff = left.add_by_ref(&right.neg_by_ref());
    if diff.is_zero() {
        return true;
    }
    let mut cursor = diff.cursor();
    while cursor.key_valid() {
        let weight = **cursor.weight();
        let key = cursor.key();
        if weight.le0() {
            println!("R: {:?}x{:?}", key, weight.neg());
        } else {
            println!("L: {:?}x{:?}", key, weight);
        }
        cursor.step_key();
    }
    false
}
