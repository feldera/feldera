use std::{
    cmp::Ordering,
    fmt::{self, Debug, Formatter},
    hash::{Hash, Hasher},
    marker::PhantomData,
};

use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;

/// Custom comparison function.
pub trait CmpFunc<T>: Send + 'static {
    fn cmp(left: &T, right: &T) -> Ordering;
}

/// Wrapper around type `T` that uses `F` instead of
/// `Ord::cmp` to compare values.
#[derive(SizeOf, Archive, Serialize, Deserialize)]
pub struct WithCustomOrd<T, F> {
    pub val: T,
    phantom: PhantomData<F>,
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

impl<T, F> Clone for WithCustomOrd<T, F>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        Self::new(self.val.clone())
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

impl<T, F> Hash for WithCustomOrd<T, F>
where
    T: Hash,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.val.hash(state);
    }
}

impl<T, F> Eq for WithCustomOrd<T, F> where T: PartialEq {}

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

impl<T, F> PartialOrd for WithCustomOrd<T, F>
where
    Self: Eq,
    F: CmpFunc<T>,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
