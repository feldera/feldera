//! Re-pointing a value's interned references at another table.
//!
//! Some leaf types keep their strings in a side table shared by all the values
//! of a batch, so that a string that repeats across rows is stored once. Such a
//! value is compact but tied to the table it was built against, and a batch
//! builder needs a way to move it onto a table of its own.
//!
//! This module supplies the plumbing without knowing what a side table is.
//! [`Interned`] sits in [`DBData`](crate::DBData)'s bounds and reaches every
//! leaf of a row through the derive; the session it carries is opaque, created
//! by whichever leaf type needs one.
//!
//! Almost every type has nothing to re-point. `MAY_INTERN` is a constant so a
//! builder can decide once per batch, not once per row, and
//! [`never_interned!`] gives the empty implementation to a list of types the
//! way [`never_roaring_filter!`](crate::never_roaring_filter) does.

use std::any::Any;
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;

use crate::dynamic::{BSet, DataTrait, DynVec, LeanVec};
use crate::time::UnitTimestamp;

/// Move every value in `values` onto a side table of its own.
///
/// This is the operation a batch builder performs once its values are in
/// place. The session is driven in the two passes it expects: gather every
/// value, lay the table out, then rewrite every value in the same order.
///
/// Returns without touching anything when the value type has nothing to
/// intern, which is the overwhelming majority of them, or when the batch is
/// empty.
pub fn reintern_values<T: DataTrait + ?Sized>(values: &mut DynVec<T>) {
    let Some(first) = values.first() else {
        return;
    };
    if !first.may_intern() {
        return;
    }
    let Some(mut session) = first.new_intern_session_dyn() else {
        return;
    };
    for i in 0..values.len() {
        values.index_mut(i).reintern_dyn(&mut *session);
    }
    session.begin_apply();
    for i in 0..values.len() {
        values.index_mut(i).reintern_dyn(&mut *session);
    }
}

/// A batch-scoped place to move interned references into.
///
/// The concrete type belongs to whichever leaf needs it; everything between the
/// builder and the leaf passes it along untouched.
///
/// A builder drives a session in two passes over the same values in the same
/// order. The first gathers what the values hold, because a side table can only
/// be laid out once its whole contents are known. Then the builder calls
/// [`begin_apply`](InternSession::begin_apply) and visits the values again, and
/// the second pass rewrites them.
pub trait InternSession: Any + Send {
    fn as_any_mut(&mut self) -> &mut dyn Any;

    /// Close the gathering pass and lay out the side table. The builder calls
    /// this once, after visiting every value and before visiting them again.
    fn begin_apply(&mut self);
}

/// Values that may hold references into a shared side table.
pub trait Interned {
    /// Whether this type can transitively hold interned references.
    ///
    /// A constant, so a builder tests it once per batch rather than once per
    /// row and the whole mechanism costs nothing for the types that have no
    /// strings to share.
    const MAY_INTERN: bool = false;

    /// Create the session this type's leaves need, or `None` when there is
    /// nothing to intern.
    fn new_intern_session() -> Option<Box<dyn InternSession>> {
        None
    }

    /// Re-point every interned reference in `self` at `session`.
    fn reintern(&mut self, session: &mut dyn InternSession) {
        let _ = session;
    }
}

/// Give a list of types the empty implementation: nothing to re-point.
#[macro_export]
macro_rules! never_interned {
    ($($ty:ty),* $(,)?) => {
        $(
            impl $crate::dynamic::Interned for $ty {}
        )*
    };
}

/// Give a generic wrapper the empty implementation, for containers whose
/// contents cannot hold interned references.
#[macro_export]
macro_rules! never_interned_1 {
    ($($wrapper:ident),* $(,)?) => {
        $(
            impl<T> $crate::dynamic::Interned for $wrapper<T> {}
        )*
    };
}

/// Forward to every element of a sequence.
macro_rules! forward_interned_seq {
    ($($wrapper:ident),* $(,)?) => {
        $(
            impl<T: Interned> Interned for $wrapper<T> {
                const MAY_INTERN: bool = T::MAY_INTERN;

                fn new_intern_session() -> Option<Box<dyn InternSession>> {
                    T::new_intern_session()
                }

                fn reintern(&mut self, session: &mut dyn InternSession) {
                    for value in self.as_mut_slice() {
                        value.reintern(session);
                    }
                }
            }
        )*
    };
}

never_interned!(
    (),
    bool,
    char,
    i8,
    i16,
    i32,
    i64,
    i128,
    isize,
    u8,
    u16,
    u32,
    u64,
    u128,
    usize,
    f32,
    f64,
    String,
    UnitTimestamp,
    uuid::Uuid,
);

never_interned_1!(BSet, PhantomData);

impl<T> Interned for ordered_float::OrderedFloat<T> {}

// A fixed-size array of things that cannot be re-pointed cannot either. The
// generic form would need `[T: Interned; N]`, which no `DBData` type needs.
impl<T: Interned, const N: usize> Interned for [T; N] {
    const MAY_INTERN: bool = T::MAY_INTERN;

    fn new_intern_session() -> Option<Box<dyn InternSession>> {
        T::new_intern_session()
    }

    fn reintern(&mut self, session: &mut dyn InternSession) {
        for value in self.iter_mut() {
            value.reintern(session);
        }
    }
}

impl<T: Interned> Interned for Option<T> {
    const MAY_INTERN: bool = T::MAY_INTERN;

    fn new_intern_session() -> Option<Box<dyn InternSession>> {
        T::new_intern_session()
    }

    fn reintern(&mut self, session: &mut dyn InternSession) {
        if let Some(value) = self {
            value.reintern(session);
        }
    }
}

forward_interned_seq!(Vec, LeanVec);

impl<T: Interned> Interned for Box<T> {
    const MAY_INTERN: bool = T::MAY_INTERN;

    fn new_intern_session() -> Option<Box<dyn InternSession>> {
        T::new_intern_session()
    }

    fn reintern(&mut self, session: &mut dyn InternSession) {
        (**self).reintern(session);
    }
}

// A shared value cannot be re-pointed in place without disturbing its other
// holders, so these keep the empty implementation. No `DBData` type in the
// tree reaches an interning leaf through one.
impl<T> Interned for Arc<T> {}
impl<T> Interned for Rc<T> {}
impl<K, V> Interned for BTreeMap<K, V> {}

macro_rules! interned_tuples {
    ($($name:ident),+) => {
        impl<$($name: Interned),+> Interned for ($($name,)+) {
            const MAY_INTERN: bool = $($name::MAY_INTERN ||)+ false;

            fn new_intern_session() -> Option<Box<dyn InternSession>> {
                None $(.or_else(|| if $name::MAY_INTERN {
                    $name::new_intern_session()
                } else {
                    None
                }))+
            }

            fn reintern(&mut self, session: &mut dyn InternSession) {
                #[allow(non_snake_case)]
                let ($($name,)+) = self;
                $($name.reintern(session);)+
            }
        }
    };
}

interned_tuples!(A);
interned_tuples!(A, B);
interned_tuples!(A, B, C);
interned_tuples!(A, B, C, D);
interned_tuples!(A, B, C, D, E);
interned_tuples!(A, B, C, D, E, F);
interned_tuples!(A, B, C, D, E, F, G);
interned_tuples!(A, B, C, D, E, F, G, H);

#[cfg(test)]
mod tests {
    use super::*;

    /// A leaf that counts how often it is re-pointed, standing in for a type
    /// with a real side table.
    #[derive(Default)]
    struct Leaf(u32);

    struct Counter(u32);

    impl InternSession for Counter {
        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }

        fn begin_apply(&mut self) {
            self.0 = 0;
        }
    }

    impl Interned for Leaf {
        const MAY_INTERN: bool = true;

        fn new_intern_session() -> Option<Box<dyn InternSession>> {
            Some(Box::new(Counter(0)))
        }

        fn reintern(&mut self, session: &mut dyn InternSession) {
            let counter = session
                .as_any_mut()
                .downcast_mut::<Counter>()
                .expect("a Leaf is re-pointed with its own session");
            counter.0 += 1;
            self.0 = counter.0;
        }
    }

    /// The flag has to be true for a compound whose leaves intern, and false
    /// for one whose leaves do not, because that is what lets a builder skip
    /// the visit for the overwhelming majority of key types.
    #[test]
    fn may_intern_propagates_through_compounds() {
        // Const blocks, because the whole point of `MAY_INTERN` being a
        // constant is that a builder can branch on it without a value in hand.
        const {
            assert!(!<(u32, String)>::MAY_INTERN);
            assert!(<(u32, Leaf)>::MAY_INTERN);
            assert!(<Option<Leaf>>::MAY_INTERN);
            assert!(<Vec<Option<Leaf>>>::MAY_INTERN);
            assert!(!<Vec<Option<u32>>>::MAY_INTERN);
            assert!(<(u8, (u8, Vec<Leaf>))>::MAY_INTERN);
        }
    }

    /// Every leaf of a row must be reached, and all of them must share the one
    /// session the builder created.
    #[test]
    fn reintern_reaches_every_leaf_with_one_session() {
        let mut row = (
            1u32,
            Leaf::default(),
            Some(Leaf::default()),
            vec![Leaf::default(), Leaf::default()],
            None::<Leaf>,
        );
        let mut session =
            <(u32, Leaf, Option<Leaf>, Vec<Leaf>, Option<Leaf>)>::new_intern_session()
                .expect("a row holding a Leaf needs a session");
        row.reintern(&mut *session);
        assert_eq!((row.1.0, row.2.as_ref().unwrap().0), (1, 2));
        assert_eq!((row.3[0].0, row.3[1].0), (3, 4));

        // The second pass visits the same leaves in the same order.
        session.begin_apply();
        row.reintern(&mut *session);
        assert_eq!((row.1.0, row.2.as_ref().unwrap().0), (1, 2));
        assert_eq!((row.3[0].0, row.3[1].0), (3, 4));
    }

    #[test]
    fn plain_types_need_no_session() {
        assert!(<(u32, String, Option<u64>)>::new_intern_session().is_none());
    }
}
