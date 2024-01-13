//! Tuple types for which we control trait implementations.

// This was introduced to resolve issues with auto-derived rkyv trait
// implementations.

// TODO: use a macro to generate Tup3, Tup4, ... if needed,
// but for now only 2-tuples are useful.

// TODO: Unify this with the `TupleN` types from the SQL compiler
// (those types are meant to be instantiated on-demand for each N,
// so we'll have to avoid potential conflict due to instantiating
// The types multiple times).

use std::fmt::{self, Debug, Formatter};

use rkyv::Archive;
use size_of::SizeOf;

/// A two-tuple.
#[derive(
    Clone,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    SizeOf,
    Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    serde::Serialize,
    serde::Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
#[archive(bound(
    archive = "T1: Archive, T2: Archive, <T1 as Archive>::Archived: Ord, <T2 as Archive>::Archived: Ord"
))]
#[archive(compare(PartialEq, PartialOrd))]
pub struct Tup2<T1, T2>(pub T1, pub T2);

impl<T1: Copy, T2: Copy> Copy for Tup2<T1, T2> {}

impl<T1, T2> From<(T1, T2)> for Tup2<T1, T2> {
    fn from((t1, t2): (T1, T2)) -> Self {
        Self(t1, t2)
    }
}

impl<T1: Debug, T2: Debug> Debug for Tup2<T1, T2> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Tup2").field(&self.0).field(&self.1).finish()
    }
}

/// A three-tuple.
#[derive(
    Clone,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    SizeOf,
    Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    serde::Serialize,
    serde::Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
#[archive(bound(
    archive = "T1: Archive, T2: Archive, T3: Archive, <T1 as Archive>::Archived: Ord, <T2 as Archive>::Archived: Ord, <T3 as Archive>::Archived: Ord"
))]
#[archive(compare(PartialEq, PartialOrd))]
pub struct Tup3<T1, T2, T3>(pub T1, pub T2, pub T3);

impl<T1: Copy, T2: Copy, T3: Copy> Copy for Tup3<T1, T2, T3> {}

impl<T1, T2, T3> From<(T1, T2, T3)> for Tup3<T1, T2, T3> {
    fn from((t1, t2, t3): (T1, T2, T3)) -> Self {
        Self(t1, t2, t3)
    }
}

impl<T1: Debug, T2: Debug, T3: Debug> Debug for Tup3<T1, T2, T3> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Tup3")
            .field(&self.0)
            .field(&self.1)
            .field(&self.2)
            .finish()
    }
}

/// A four-tuple.
#[derive(
    Clone,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    SizeOf,
    Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    serde::Serialize,
    serde::Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
#[archive(bound(archive = "T1: Archive, T2: Archive, T3: Archive, T4: Archive, \
    <T1 as Archive>::Archived: Ord, \
    <T2 as Archive>::Archived: Ord, \
    <T3 as Archive>::Archived: Ord, \
    <T4 as Archive>::Archived: Ord"))]
#[archive(compare(PartialEq, PartialOrd))]
pub struct Tup4<T1, T2, T3, T4>(pub T1, pub T2, pub T3, pub T4);

impl<T1, T2, T3, T4> From<(T1, T2, T3, T4)> for Tup4<T1, T2, T3, T4> {
    fn from((t1, t2, t3, t4): (T1, T2, T3, T4)) -> Self {
        Self(t1, t2, t3, t4)
    }
}

impl<T1: Debug, T2: Debug, T3: Debug, T4: Debug> Debug for Tup4<T1, T2, T3, T4> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Tup4")
            .field(&self.0)
            .field(&self.1)
            .field(&self.2)
            .field(&self.3)
            .finish()
    }
}

/// A five-tuple.
#[derive(
    Clone,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    SizeOf,
    Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    serde::Serialize,
    serde::Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
#[archive(bound(archive = "T1: Archive, T2: Archive, T3: Archive, T4: Archive, \
    <T1 as Archive>::Archived: Ord, \
    <T2 as Archive>::Archived: Ord, \
    <T3 as Archive>::Archived: Ord, \
    <T4 as Archive>::Archived: Ord, \
    <T5 as Archive>::Archived: Ord"))]
#[archive(compare(PartialEq, PartialOrd))]
pub struct Tup5<T1, T2, T3, T4, T5>(pub T1, pub T2, pub T3, pub T4, pub T5);

impl<T1, T2, T3, T4, T5> From<(T1, T2, T3, T4, T5)> for Tup5<T1, T2, T3, T4, T5> {
    fn from((t1, t2, t3, t4, t5): (T1, T2, T3, T4, T5)) -> Self {
        Self(t1, t2, t3, t4, t5)
    }
}

impl<T1: Debug, T2: Debug, T3: Debug, T4: Debug, T5: Debug> Debug for Tup5<T1, T2, T3, T4, T5> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Tup5")
            .field(&self.0)
            .field(&self.1)
            .field(&self.2)
            .field(&self.3)
            .field(&self.4)
            .finish()
    }
}

/// A six-tuple.
#[derive(
    Clone,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    SizeOf,
    Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    serde::Serialize,
    serde::Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
#[archive(bound(
    archive = "T1: Archive, T2: Archive, T3: Archive, T4: Archive, T5: Archive, T6: Archive, \
    <T1 as Archive>::Archived: Ord, \
    <T2 as Archive>::Archived: Ord, \
    <T3 as Archive>::Archived: Ord, \
    <T4 as Archive>::Archived: Ord, \
    <T5 as Archive>::Archived: Ord,\
    <T6 as Archive>::Archived: Ord,
"
))]
#[archive(compare(PartialEq, PartialOrd))]
pub struct Tup6<T1, T2, T3, T4, T5, T6>(pub T1, pub T2, pub T3, pub T4, pub T5, pub T6);

impl<T1, T2, T3, T4, T5, T6> From<(T1, T2, T3, T4, T5, T6)> for Tup6<T1, T2, T3, T4, T5, T6> {
    fn from((t1, t2, t3, t4, t5, t6): (T1, T2, T3, T4, T5, T6)) -> Self {
        Self(t1, t2, t3, t4, t5, t6)
    }
}

impl<T1: Debug, T2: Debug, T3: Debug, T4: Debug, T5: Debug, T6: Debug> Debug
    for Tup6<T1, T2, T3, T4, T5, T6>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Tup6")
            .field(&self.0)
            .field(&self.1)
            .field(&self.2)
            .field(&self.3)
            .field(&self.4)
            .field(&self.5)
            .finish()
    }
}

/// A seven-tuple.
#[derive(
    Clone,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    SizeOf,
    Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    serde::Serialize,
    serde::Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
#[archive(bound(
    archive = "T1: Archive, T2: Archive, T3: Archive, T4: Archive, T5: Archive, T6: Archive, T7: Archive, \
    <T1 as Archive>::Archived: Ord, \
    <T2 as Archive>::Archived: Ord, \
    <T3 as Archive>::Archived: Ord, \
    <T4 as Archive>::Archived: Ord, \
    <T5 as Archive>::Archived: Ord,\
    <T6 as Archive>::Archived: Ord, \
    <T7 as Archive>::Archived: Ord, \
"
))]
#[archive(compare(PartialEq, PartialOrd))]
pub struct Tup7<T1, T2, T3, T4, T5, T6, T7>(pub T1, pub T2, pub T3, pub T4, pub T5, pub T6, pub T7);

impl<T1, T2, T3, T4, T5, T6, T7> From<(T1, T2, T3, T4, T5, T6, T7)>
    for Tup7<T1, T2, T3, T4, T5, T6, T7>
{
    fn from((t1, t2, t3, t4, t5, t6, t7): (T1, T2, T3, T4, T5, T6, T7)) -> Self {
        Self(t1, t2, t3, t4, t5, t6, t7)
    }
}

impl<T1: Debug, T2: Debug, T3: Debug, T4: Debug, T5: Debug, T6: Debug, T7: Debug> Debug
    for Tup7<T1, T2, T3, T4, T5, T6, T7>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Tup7")
            .field(&self.0)
            .field(&self.1)
            .field(&self.2)
            .field(&self.3)
            .field(&self.4)
            .field(&self.5)
            .field(&self.6)
            .finish()
    }
}

/// A eight-tuple.
#[derive(
    Clone,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    SizeOf,
    Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    serde::Serialize,
    serde::Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
#[archive(bound(
    archive = "T1: Archive, T2: Archive, T3: Archive, T4: Archive, T5: Archive, T6: Archive, T7: Archive, T8: Archive, \
    <T1 as Archive>::Archived: Ord, \
    <T2 as Archive>::Archived: Ord, \
    <T3 as Archive>::Archived: Ord, \
    <T4 as Archive>::Archived: Ord, \
    <T5 as Archive>::Archived: Ord, \
    <T6 as Archive>::Archived: Ord, \
    <T7 as Archive>::Archived: Ord, \
    <T8 as Archive>::Archived: Ord"
))]
#[archive(compare(PartialEq, PartialOrd))]
pub struct Tup8<T1, T2, T3, T4, T5, T6, T7, T8>(
    pub T1,
    pub T2,
    pub T3,
    pub T4,
    pub T5,
    pub T6,
    pub T7,
    pub T8,
);

impl<T1, T2, T3, T4, T5, T6, T7, T8> From<(T1, T2, T3, T4, T5, T6, T7, T8)>
    for Tup8<T1, T2, T3, T4, T5, T6, T7, T8>
{
    fn from((t1, t2, t3, t4, t5, t6, t7, t8): (T1, T2, T3, T4, T5, T6, T7, T8)) -> Self {
        Self(t1, t2, t3, t4, t5, t6, t7, t8)
    }
}

impl<T1: Debug, T2: Debug, T3: Debug, T4: Debug, T5: Debug, T6: Debug, T7: Debug, T8: Debug> Debug
    for Tup8<T1, T2, T3, T4, T5, T6, T7, T8>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Tup8")
            .field(&self.0)
            .field(&self.1)
            .field(&self.2)
            .field(&self.3)
            .field(&self.4)
            .field(&self.5)
            .field(&self.6)
            .field(&self.7)
            .finish()
    }
}

/// A nine-tuple.
#[derive(
    Clone,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    SizeOf,
    Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    serde::Serialize,
    serde::Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
#[archive(bound(
    archive = "T1: Archive, T2: Archive, T3: Archive, T4: Archive, T5: Archive, T6: Archive, T7: Archive, T8: Archive, T9: Archive, \
    <T1 as Archive>::Archived: Ord, \
    <T2 as Archive>::Archived: Ord, \
    <T3 as Archive>::Archived: Ord, \
    <T4 as Archive>::Archived: Ord, \
    <T5 as Archive>::Archived: Ord, \
    <T6 as Archive>::Archived: Ord, \
    <T7 as Archive>::Archived: Ord, \
    <T8 as Archive>::Archived: Ord, \
    <T9 as Archive>::Archived: Ord"
))]
#[archive(compare(PartialEq, PartialOrd))]
pub struct Tup9<T1, T2, T3, T4, T5, T6, T7, T8, T9>(
    pub T1,
    pub T2,
    pub T3,
    pub T4,
    pub T5,
    pub T6,
    pub T7,
    pub T8,
    pub T9,
);

impl<T1, T2, T3, T4, T5, T6, T7, T8, T9> From<(T1, T2, T3, T4, T5, T6, T7, T8, T9)>
    for Tup9<T1, T2, T3, T4, T5, T6, T7, T8, T9>
{
    fn from((t1, t2, t3, t4, t5, t6, t7, t8, t9): (T1, T2, T3, T4, T5, T6, T7, T8, T9)) -> Self {
        Self(t1, t2, t3, t4, t5, t6, t7, t8, t9)
    }
}

impl<
        T1: Debug,
        T2: Debug,
        T3: Debug,
        T4: Debug,
        T5: Debug,
        T6: Debug,
        T7: Debug,
        T8: Debug,
        T9: Debug,
    > Debug for Tup9<T1, T2, T3, T4, T5, T6, T7, T8, T9>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Tup9")
            .field(&self.0)
            .field(&self.1)
            .field(&self.2)
            .field(&self.3)
            .field(&self.4)
            .field(&self.5)
            .field(&self.6)
            .field(&self.7)
            .field(&self.8)
            .finish()
    }
}

/// A ten-tuple.
#[derive(
    Clone,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    SizeOf,
    Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    serde::Serialize,
    serde::Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
#[archive(bound(
    archive = "T1: Archive, T2: Archive, T3: Archive, T4: Archive, T5: Archive, T6: Archive, T7: Archive, T8: Archive, T9: Archive, T10: Archive, \
    <T1 as Archive>::Archived: Ord, \
    <T2 as Archive>::Archived: Ord, \
    <T3 as Archive>::Archived: Ord, \
    <T4 as Archive>::Archived: Ord, \
    <T5 as Archive>::Archived: Ord, \
    <T6 as Archive>::Archived: Ord, \
    <T7 as Archive>::Archived: Ord, \
    <T8 as Archive>::Archived: Ord, \
    <T9 as Archive>::Archived: Ord, \
    <T10 as Archive>::Archived: Ord"
))]
#[archive(compare(PartialEq, PartialOrd))]
pub struct Tup10<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>(
    pub T1,
    pub T2,
    pub T3,
    pub T4,
    pub T5,
    pub T6,
    pub T7,
    pub T8,
    pub T9,
    pub T10,
);

impl<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10> From<(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)>
    for Tup10<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>
{
    fn from(
        (t1, t2, t3, t4, t5, t6, t7, t8, t9, t10): (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10),
    ) -> Self {
        Self(t1, t2, t3, t4, t5, t6, t7, t8, t9, t10)
    }
}

impl<
        T1: Debug,
        T2: Debug,
        T3: Debug,
        T4: Debug,
        T5: Debug,
        T6: Debug,
        T7: Debug,
        T8: Debug,
        T9: Debug,
        T10: Debug,
    > Debug for Tup10<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Tup10")
            .field(&self.0)
            .field(&self.1)
            .field(&self.2)
            .field(&self.3)
            .field(&self.4)
            .field(&self.5)
            .field(&self.6)
            .field(&self.7)
            .field(&self.8)
            .field(&self.9)
            .finish()
    }
}
