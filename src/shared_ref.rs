//! Shared reference trait.

use std::{borrow::Borrow, rc::Rc};

/// A trait that generalizes shared pointers like `Rc` and `Arc`.
///
/// The holder of a shared reference can extract an owned copy
/// of the reference object from it if there are no other references
/// to the same object (the refcount is 0).
pub trait SharedRef<T>: Borrow<T> + Clone + Sized {
    /// Try to extract the owned value from `self`.
    ///
    /// If `self` is the last reference to the object, returns
    /// the object; otherwise, returns `Err(self)`.
    fn try_into_owned(self) -> Result<T, Self>;
}

impl<T> SharedRef<T> for T
where
    T: Clone,
{
    fn try_into_owned(self) -> Result<T, Self> {
        Ok(self)
    }
}

impl<T> SharedRef<T> for Rc<T> {
    fn try_into_owned(self) -> Result<T, Self> {
        Rc::try_unwrap(self)
    }
}
