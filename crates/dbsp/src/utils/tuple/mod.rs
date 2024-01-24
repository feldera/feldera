//! Tuple types for which we control trait implementations.

#![allow(non_snake_case)]

// This was introduced to resolve issues with auto-derived rkyv trait
// implementations.

pub mod gen;

// Make sure to also call `dbsp_adapters::deserialize_without_context!`
// and `sqlvalue::to_sql_row_impl!` for each new tuple type.
// Also the compiler currently generates Tup11..Tup* if necessary,
// so if e.g., we add Tup11 here the compiler needs to be adjusted too.
crate::declare_tuples! {
    Tup1<T1>,
    Tup2<T1, T2>,
    Tup3<T1, T2, T3>,
    Tup4<T1, T2, T3, T4>,
    Tup5<T1, T2, T3, T4, T5>,
    Tup6<T1, T2, T3, T4, T5, T6>,
    Tup7<T1, T2, T3, T4, T5, T6, T7>,
    Tup8<T1, T2, T3, T4, T5, T6, T7, T8>,
    Tup9<T1, T2, T3, T4, T5, T6, T7, T8, T9>,
    Tup10<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>,
}
