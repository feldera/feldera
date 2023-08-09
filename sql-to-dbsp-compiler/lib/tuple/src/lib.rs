//! This file contains a macro which can be used to define tuples
//! with any number of fields.  The type names are `Tuple0<>`, `Tuple1<T0>`,
//! `Tuple2<T0, T1>`, etc.  The macro defines many traits which
//! are useful for tuples to be used as DBSP values in Z-Sets.
//! Rust tuples only go up to 12 fields, but we may need more.

#[macro_export]
macro_rules! declare_tuples {
    (
        $(
            $tuple_name:ident<$($element:tt),* $(,)?>
        ),*
        $(,)?
    ) => {
        $(
            #[derive(Default, Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, SizeOf, Add, Neg, AddAssign, Encode, Decode)]
            pub struct $tuple_name<$($element,)*>($(pub $element,)*);

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
             impl<T0: Copy, T1: Copy> Copy for Tuple2<T0, T1> {}
             pub struct Tuple3<T0, T1, T2>(pub T0, pub T1, pub T2);
            */
            impl<$($element: Copy),*> Copy for $tuple_name<$($element),*> {}
        )*
    };
}
