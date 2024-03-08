//! This file contains a macro which can be used to define tuples with any
//! number of fields.  The type names are `Tuple0<>`, `Tuple1<T0>`,
//! `Tuple2<T0, T1>`, etc.
//!
//! The macro defines many traits which are useful for tuples to be used as DBSP
//! values in Z-Sets. Rust tuples only go up to 12 fields, but we may need more.

#[macro_export]
macro_rules! count_items {
    () => { 0usize };
    ($first:ident) => { 1usize };
    ($first:ident, $($rest:ident),*) => {
        1usize + $crate::count_items!($($rest),*)
    }
}

#[macro_export]
macro_rules! measure_items {
    () => { 0usize };
    ($first:expr) => { $first.num_entries_deep() };
    ($first:expr, $($rest:expr),*) => {
        $first.num_entries_deep() + $crate::measure_items!($($rest),*)
    }
}

#[macro_export]
macro_rules! declare_tuples {
    (
        $(
            $tuple_name:ident<$($element:tt),* $(,)?>
        ),*
        $(,)?
    ) => {
        $(
            paste::paste! {
                #[derive(
                    Default,
                    Eq,
                    Ord,
                    Clone,
                    Hash,
                    PartialEq,
                    PartialOrd,
                    derive_more::Add,
                    derive_more::Neg,
                    derive_more::AddAssign,
                    serde::Serialize,
                    serde::Deserialize,
                    size_of::SizeOf,
                    rkyv::Archive,
                    rkyv::Serialize,
                    rkyv::Deserialize
                )]
                #[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
                #[archive(bound(
                    archive = $( "" $element "" ": rkyv::Archive, ")* "" $( "<" $element "" " as rkyv::Archive>::Archived: Ord, ")*
                ))]
                pub struct $tuple_name<$($element,)*>($(pub $element,)*);
            }

            /*$(<$element as Archive>::Archived: Ord, )*
            Expands to:
            impl<T0, T1> Tup2<T0, T1> {
               fn new(T0: T0, T1: T1) -> Self {
                  Self(T0, T1)
               }
            }
            */
            impl<$($element),*> $tuple_name<$($element,)*>
            {
                #[allow(clippy::too_many_arguments)]
                pub fn new($($element: $element),*) -> Self {
                    Self($($element),*)
                }
            }

            /*
             Example generated code:
             impl<T0, T1, W> $crate::algebra::MulByRef<W> for Tup2<T0, T1>
             where
                 T0: $crate::algebra::MulByRef<W, Output = T0>,
                 T1: $crate::algebra::MulByRef<W, Output = T1>,
                 W: $crate::algebra::ZRingValue,
             {
                 type Output = Self;
                 fn mul_by_ref(&self, other: &W) -> Self::Output {
                     let Tup2(T0, T1) = self;
                     Tup2(T0.mul_by_ref(other), T1.mul_by_ref(other))
                 }
             }
             */
            impl<$($element),*, W> $crate::algebra::MulByRef<W>
                for $tuple_name<$($element,)*>
            where
                $($element: $crate::algebra::MulByRef<W, Output=$element>,)*
                W: $crate::algebra::ZRingValue
            {
                type Output = Self;
                fn mul_by_ref(&self, other: &W) -> Self::Output {
                    let $tuple_name($($element),*) = self;
                    $tuple_name($($element.mul_by_ref(other),)*)
                }
            }

            /*
             Example generated code:
             impl<T0, T1> $crate::algebra::HasZero for Tup2<T0, T1>
             where
                 T0: $crate::algebra::HasZero,
                 T1: $crate::algebra::HasZero,
             {
                 fn zero() -> Self {
                     Tup2(T0::zero(), T1::zero())
                 }
                 fn is_zero(&self) -> bool {
                     let mut result = true;
                     let Tup2(T0, T1) = self;
                     result = result && T0.is_zero();
                     result = result && T1.is_zero();
                     result
                 }
             }
             */
            impl<$($element),*> $crate::algebra::HasZero
                for $tuple_name<$($element,)*>
            where
                $($element: $crate::algebra::HasZero,)*
            {
                fn zero() -> Self {
                    $tuple_name($($element::zero(),)*)
                }
                fn is_zero(&self) -> bool {
                    let mut result = true;
                    let $tuple_name($($element),*) = self;
                    $(result = result && $element.is_zero();)*
                    result
                }
            }

            /*
             Example generated code:
             impl<T0, T1> $crate::algebra::AddByRef for Tup2<T0, T1>
             where
                 T0: $crate::algebra::AddByRef,
                 T1: $crate::algebra::AddByRef,
             {
                 fn add_by_ref(&self, other: &Self) -> Self {
                     let Tup2(T0, T1) = self;
                     let Tup2(QT0, QT1) = other;
                     Tup2(T0.add_by_ref(QT0), T1.add_by_ref(QT1))
                 }
             }
            */
            impl<$($element),*> $crate::algebra::AddByRef
                for $tuple_name<$($element,)*>
            where
                $($element: $crate::algebra::AddByRef,)*
            {
                fn add_by_ref(&self, other: &Self) -> Self {
                    let $tuple_name($($element),*) = self;
                    let $tuple_name($(paste::paste!( [<Q $element>])),*) = other;
                    $tuple_name($($element.add_by_ref(paste::paste!( [<Q $element>])),)*)
                }
            }

            /*
            Example generated code:
            impl<T0, T1> $crate::NumEntries for Tup2<T0, T1>
            where
                T0: $crate::NumEntries,
                T1: $crate::NumEntries,
            {
                const CONST_NUM_ENTRIES: Option<usize> = None;
                fn num_entries_shallow(&self) -> usize {
                    1usize + 1usize
                }
                fn num_entries_deep(&self) -> usize {
                    let Tup2(T0, T1) = self;
                    T0.num_entries_deep() + T1.num_entries_deep()
                }
            }
             */
            impl<$($element),*> $crate::NumEntries
                for $tuple_name<$($element,)*>
            where
                $($element: $crate::NumEntries,)*
            {
                const CONST_NUM_ENTRIES: Option<usize> = None;

                fn num_entries_shallow(&self) -> usize {
                    $crate::count_items!($($element),*)
                }

                fn num_entries_deep(&self) -> usize {
                    let $tuple_name($($element),*) = self;
                    $crate::measure_items!($($element),*)
                }
            }

            /*
             Example generated code:
             impl<T0, T1> $crate::algebra::AddAssignByRef for Tup2<T0, T1>
             where
                 T0: $crate::algebra::AddAssignByRef,
                 T1: $crate::algebra::AddAssignByRef,
             {
                 fn add_assign_by_ref(&mut self, other: &Self) {
                     let Tup2(T0, T1) = self;
                     let Tup2(QT0, QT1) = other;
                     T0.add_assign_by_ref(&QT0);
                     T1.add_assign_by_ref(&QT1);
                 }
             }
             */
            impl<$($element),*> $crate::algebra::AddAssignByRef
                for $tuple_name<$($element,)*>
            where
                $($element: $crate::algebra::AddAssignByRef,)*
            {
                fn add_assign_by_ref(&mut self, other: &Self) {
                    let $tuple_name($($element),*) = self;
                    let $tuple_name($(paste::paste!( [<Q $element>])),*) = other;
                    $($element.add_assign_by_ref(paste::paste!(& [<Q $element>]));)*
                }
            }

            /*
             Example generated code:
             impl<T0, T1> $crate::algebra::NegByRef for Tup2<T0, T1>
             where
                 T0: $crate::algebra::NegByRef,
                 T1: $crate::algebra::NegByRef,
             {
                 fn neg_by_ref(&self) -> Self {
                     let mut result = true;
                     let Tup2(T0, T1) = self;
                     Tup2(T0.neg_by_ref(), T1.neg_by_ref())
                 }
             }
            */
            impl<$($element),*> $crate::algebra::NegByRef
                for $tuple_name<$($element,)*>
            where
                $($element: $crate::algebra::NegByRef,)*
            {
                fn neg_by_ref(&self) -> Self {
                    let $tuple_name($($element),*) = self;
                    $tuple_name($($element.neg_by_ref(),)*)
                }
            }

            /*
             Example generated code:
             impl<T0, T1> From<(T0, T1)> for Tup2<T0, T1> {
                 fn from((T0, T1): (T0, T1)) -> Self {
                     Self(T0, T1)
                 }
             }
            */
            impl<$($element),*> From<($($element,)*)> for $tuple_name<$($element,)*> {
                fn from(($($element,)*): ($($element,)*)) -> Self {
                    Self($($element),*)
                }
            }

            /*
             Example generated code:
             impl<T0, T1> Into<(T0, T1)> for Tup2<T0, T1> {
                 fn into(self) -> (T0, T1) {
                     let Tup2(T0, T1) = self;
                     (T0, T1)
                 }
             }
             */
            #[allow(clippy::from_over_into)]
            impl<$($element),*> Into<($($element,)*)> for $tuple_name<$($element,)*> {
                fn into(self) -> ($($element,)*) {
                    let $tuple_name($($element),*) = self;
                    ($($element,)*)
                }
            }

            /*
             Example generated code:
             impl<T0: core::fmt::Debug, T1: core::fmt::Debug> core::fmt::Debug for Tup2<T0, T1> {
                 fn fmt(&self, f: &mut core::fmt::Formatter) -> core::result::Result<(), core::fmt::Error> {
                     let Tup2(T0, T1) = self;
                     f.debug_tuple("").field(&T0).field(&T1).finish()
                 }
             }
             */
            impl<$($element: core::fmt::Debug),*> core::fmt::Debug for $tuple_name<$($element),*> {
                fn fmt(&self, f: &mut core::fmt::Formatter) -> core::result::Result<(), core::fmt::Error>  {
                    let $tuple_name($($element),*) = self;
                    f.debug_tuple("")
                        $(.field(&$element))*
                        .finish()
                }
            }

            /*
             Example generated code:
             impl<T0: Copy, T1: Copy> Copy for Tup2<T0, T1> {}
            */
            impl<$($element: Copy),*> Copy for $tuple_name<$($element),*> {}
        )*
    };
}
