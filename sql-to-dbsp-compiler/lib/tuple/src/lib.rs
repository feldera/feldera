//! This file contains a macro which can be used to define tuples
//! with any number of fields.  The type names are `Tuple0<>`, `Tuple1<T0>`,
//! `Tuple2<T0, T1>`, etc.  The macro defines many traits which
//! are useful for tuples to be used as DBSP values in Z-Sets.
//! Rust tuples only go up to 12 fields, but we may need more.

#[macro_export]
macro_rules! count_items {
    () => { 0usize };
    ($first:ident) => { 1usize };
    ($first:ident, $($rest:ident),*) => {
        1usize + count_items!($($rest),*)
    }
}

#[macro_export]
macro_rules! measure_items {
    () => { 0usize };
    ($first:expr) => { $first.num_entries_deep() };
    ($first:expr, $($rest:expr),*) => {
        $first.num_entries_deep() + measure_items!($($rest),*)
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
            #[derive(Default, Eq, Ord, Clone, Hash, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize, SizeOf, Add, Neg, AddAssign, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
            pub struct $tuple_name<$($element,)*>($(pub $element,)*);

            dbsp_adapters::deserialize_without_context!($tuple_name, $($element),*);

            /*
            Expands to:
            impl<T0, T1> Tuple2<T0, T1> {
               fn new(T0: T0, T1: T1) -> Self {
                  Self(T0, T1)
               }
            }
            */
            impl<$($element),*> $tuple_name<$($element,)*>
            {
                fn new($($element: $element),*) -> Self {
                    Self($($element),*)
                }
            }

            #[cfg(test)]
            impl<$($element),*> ToSqlRow for $tuple_name<$($element,)*>
            where
                $(SqlValue: From<$element>,)*
                $($element: Clone,)*
            {
                fn to_row(&self) -> SqlRow
                {
                    let mut result = SqlRow::new();
                    let $tuple_name($($element),*) = self;
                    $(result.push(SqlValue::from($element.clone()));)*
                    result
                }
            }

            /*
             Example generated code:
             impl<T0, T1, W> MulByRef<W> for Tuple2<T0, T1>
             where
                 T0: MulByRef<W, Output = T0>,
                 T1: MulByRef<W, Output = T1>,
                 W: ZRingValue,
             {
                 type Output = Self;
                 fn mul_by_ref(&self, other: &W) -> Self::Output {
                     let Tuple2(T0, T1) = self;
                     Tuple2(T0.mul_by_ref(other), T1.mul_by_ref(other))
                 }
             }
             */
            impl<$($element),*, W> MulByRef<W>
                for $tuple_name<$($element,)*>
            where
                $($element: MulByRef<W, Output=$element>,)*
                W: ZRingValue
            {
                type Output = Self;
                fn mul_by_ref(&self, other: &W) -> Self::Output {
                    let $tuple_name($($element),*) = self;
                    $tuple_name($($element.mul_by_ref(other),)*)
                }
            }

            /*
             Example generated code:
             impl<T0, T1> HasZero for Tuple2<T0, T1>
             where
                 T0: HasZero,
                 T1: HasZero,
             {
                 fn zero() -> Self {
                     Tuple2(T0::zero(), T1::zero())
                 }
                 fn is_zero(&self) -> bool {
                     let mut result = true;
                     let Tuple2(T0, T1) = self;
                     result = result && T0.is_zero();
                     result = result && T1.is_zero();
                     result
                 }
             }
             */
            impl<$($element),*> HasZero
                for $tuple_name<$($element,)*>
            where
                $($element: HasZero,)*
            {
                fn zero() -> Self {
                    $tuple_name($($element::zero(),)*)
                }
                fn is_zero(&self) -> bool {
                    let mut result = true;
                    let $tuple_name($($element),*) = self;
                    $(result = result && $element.is_zero();)*;
                    result
                }
            }

            /*
             Example generated code:
             impl<T0, T1> AddByRef for Tuple2<T0, T1>
             where
                 T0: AddByRef,
                 T1: AddByRef,
             {
                 fn add_by_ref(&self, other: &Self) -> Self {
                     let Tuple2(T0, T1) = self;
                     let Tuple2(QT0, QT1) = other;
                     Tuple2(T0.add_by_ref(QT0), T1.add_by_ref(QT1))
                 }
             }
            */
            impl<$($element),*> AddByRef
                for $tuple_name<$($element,)*>
            where
                $($element: AddByRef,)*
            {
                fn add_by_ref(&self, other: &Self) -> Self {
                    let $tuple_name($($element),*) = self;
                    let $tuple_name($(paste!( [<Q $element>])),*) = other;
                    $tuple_name($($element.add_by_ref(paste!( [<Q $element>])),)*)
                }
            }

            /*
            Example generated code:
            impl<T0, T1> NumEntries for Tuple2<T0, T1>
            where
                T0: NumEntries,
                T1: NumEntries,
            {
                const CONST_NUM_ENTRIES: Option<usize> = None;
                fn num_entries_shallow(&self) -> usize {
                    1usize + 1usize
                }
                fn num_entries_deep(&self) -> usize {
                    let Tuple2(T0, T1) = self;
                    T0.num_entries_deep() + T1.num_entries_deep()
                }
            }
             */
            impl<$($element),*> NumEntries
                for $tuple_name<$($element,)*>
            where
                $($element: NumEntries,)*
            {
                const CONST_NUM_ENTRIES: Option<usize> = None;

                fn num_entries_shallow(&self) -> usize {
                    count_items!($($element),*)
                }

                fn num_entries_deep(&self) -> usize {
                    let $tuple_name($($element),*) = self;
                    measure_items!($($element),*)
                }
            }

            /*
             Example generated code:
             impl<T0, T1> AddAssignByRef for Tuple2<T0, T1>
             where
                 T0: AddAssignByRef,
                 T1: AddAssignByRef,
             {
                 fn add_assign_by_ref(&mut self, other: &Self) {
                     let Tuple2(T0, T1) = self;
                     let Tuple2(QT0, QT1) = other;
                     T0.add_assign_by_ref(&QT0);
                     T1.add_assign_by_ref(&QT1);
                 }
             }
             */
            impl<$($element),*> AddAssignByRef
                for $tuple_name<$($element,)*>
            where
                $($element: AddAssignByRef,)*
            {
                fn add_assign_by_ref(&mut self, other: &Self) {
                    let $tuple_name($($element),*) = self;
                    let $tuple_name($(paste!( [<Q $element>])),*) = other;
                    $($element.add_assign_by_ref(paste!(& [<Q $element>]));)*
                }
            }

            /*
             Example generated code:
             impl<T0, T1> NegByRef for Tuple2<T0, T1>
             where
                 T0: NegByRef,
                 T1: NegByRef,
             {
                 fn neg_by_ref(&self) -> Self {
                     let mut result = true;
                     let Tuple2(T0, T1) = self;
                     Tuple2(T0.neg_by_ref(), T1.neg_by_ref())
                 }
             }
            */
            impl<$($element),*> NegByRef
                for $tuple_name<$($element,)*>
            where
                $($element: NegByRef,)*
            {
                fn neg_by_ref(&self) -> Self {
                    let mut result = true;
                    let $tuple_name($($element),*) = self;
                    $tuple_name($($element.neg_by_ref(),)*)
                }
            }

            /*
             Example generated code:
             impl<T0, T1> From<(T0, T1)> for Tuple2<T0, T1> {
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
             impl<T0, T1> Into<(T0, T1)> for Tuple2<T0, T1> {
                 fn into(self) -> (T0, T1) {
                     let Tuple2(T0, T1) = self;
                     (T0, T1)
                 }
             }
             */
            impl<$($element),*> Into<($($element,)*)> for $tuple_name<$($element,)*> {
                fn into(self) -> ($($element,)*) {
                    let $tuple_name($($element),*) = self;
                    ($($element,)*)
                }
            }

            /*
             Example generated code:
             impl<T0: Debug, T1: Debug> Debug for Tuple2<T0, T1> {
                 fn fmt(&self, f: &mut Formatter) -> FmtResult {
                     let Tuple2(T0, T1) = self;
                     f.debug_tuple("").field(&T0).field(&T1).finish()
                 }
             }
             */
            impl<$($element: Debug),*> Debug for $tuple_name<$($element),*> {
                fn fmt(&self, f: &mut Formatter) -> FmtResult {
                    let $tuple_name($($element),*) = self;
                    f.debug_tuple("")
                        $(.field(&$element))*
                        .finish()
                }
            }

            /*
             Example generated code:
             impl<T0: Copy, T1: Copy> Copy for Tuple2<T0, T1> {}
            */
            impl<$($element: Copy),*> Copy for $tuple_name<$($element),*> {}
        )*
    };
}
