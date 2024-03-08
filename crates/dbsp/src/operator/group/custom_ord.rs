use crate::trace::Deserializable;
use rkyv::{Archive, Deserialize, Infallible, Serialize};
use size_of::SizeOf;
use std::{
    cmp::Ordering,
    fmt::{self, Debug, Formatter},
    hash::{Hash, Hasher},
    marker::PhantomData,
};

/// Custom comparison function.
pub trait CmpFunc<T>: Send + 'static {
    fn cmp(left: &T, right: &T) -> Ordering;
}

/// Wrapper around type `T` that uses `F` instead of
/// `Ord::cmp` to compare values.
#[derive(SizeOf, Archive, Serialize, Deserialize)]
#[archive(compare(PartialEq, PartialOrd))]
pub struct WithCustomOrd<T, F> {
    pub val: T,
    phantom: PhantomData<F>,
}

impl<T: Default, F> Default for WithCustomOrd<T, F> {
    fn default() -> Self {
        Self {
            val: Default::default(),
            phantom: PhantomData,
        }
    }
}

impl<T, F> Debug for WithCustomOrd<T, F>
where
    T: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("WithCustomOrd")
            .field("val", &self.val)
            .finish()
    }
}

impl<T, F> Debug for ArchivedWithCustomOrd<T, F>
where
    T: Debug + Archive,
    T::Archived: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArchivedWithCustomOrd")
            .field("val", &self.val)
            .finish()
    }
}

impl<T, F> Clone for WithCustomOrd<T, F>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        Self::new(self.val.clone())
    }
}

impl<T, F> Clone for ArchivedWithCustomOrd<T, F>
where
    T: Clone + Archive,
    T::Archived: Clone,
{
    fn clone(&self) -> Self {
        ArchivedWithCustomOrd {
            val: self.val.clone(),
            phantom: PhantomData,
        }
    }
}

impl<T, F> PartialEq<Self> for WithCustomOrd<T, F>
where
    T: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.val.eq(&other.val)
    }
}

impl<T, F> PartialEq<Self> for ArchivedWithCustomOrd<T, F>
where
    T: PartialEq + Archive,
    T::Archived: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.val.eq(&other.val)
    }
}

impl<T, F> Hash for WithCustomOrd<T, F>
where
    T: Hash,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.val.hash(state);
    }
}

impl<T, F> Hash for ArchivedWithCustomOrd<T, F>
where
    T: Hash + Archive,
    T::Archived: Hash,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.val.hash(state);
    }
}

impl<T, F> Eq for WithCustomOrd<T, F> where T: PartialEq {}

impl<T, F> Eq for ArchivedWithCustomOrd<T, F>
where
    T: PartialEq + Archive,
    T::Archived: PartialEq,
{
}

impl<T, F> WithCustomOrd<T, F> {
    pub fn new(val: T) -> Self {
        Self {
            val,
            phantom: PhantomData,
        }
    }
}

impl<T, F> Ord for WithCustomOrd<T, F>
where
    Self: Eq + PartialOrd,
    F: CmpFunc<T>,
{
    fn cmp(&self, other: &Self) -> Ordering {
        F::cmp(&self.val, &other.val)
    }
}

impl<T, F> Ord for ArchivedWithCustomOrd<T, F>
where
    Self: Eq + PartialOrd,
    T: Archive + Deserializable,
    F: CmpFunc<T>,
{
    fn cmp(&self, other: &Self) -> Ordering {
        // TODO(deserialization): Not ideal -- we're deserializing the values just to
        // compare them. Better would be to have `F: CmpFunc<T::Archived>` for
        // the Archived (we already know that T::Archived implements Ord).
        let real_self: T = self.val.deserialize(&mut Infallible).unwrap();
        let real_other: T = other.val.deserialize(&mut Infallible).unwrap();
        F::cmp(&real_self, &real_other)
    }
}

impl<T, F> PartialOrd for WithCustomOrd<T, F>
where
    Self: Eq,
    F: CmpFunc<T>,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T, F> PartialOrd for ArchivedWithCustomOrd<T, F>
where
    Self: Eq,
    T: Archive + Deserializable,
    F: CmpFunc<T>,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
