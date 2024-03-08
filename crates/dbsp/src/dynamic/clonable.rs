use crate::dynamic::DowncastTrait;
use dyn_clone::DynClone;
use std::mem::take;

pub trait Clonable: DynClone {
    /// Clone `self` into `dst`.
    ///
    /// # Safety
    ///
    /// `dst` must be a valid mutable reference to a value of type `Self`.
    unsafe fn raw_clone_to(&self, dst: *mut u8);

    /// Move the contents of `self` into `dst`, replacing the contents of
    /// `self` with `Self::default()`.
    ///
    /// # Safety
    ///
    /// `dst` must be a valid mutable reference to a value of type `Self`.
    unsafe fn raw_move_to(&mut self, dst: *mut u8);

    /// Clone `src` into `self`.
    ///
    /// # Safety
    ///
    /// `src` must be a valid reference to a value of type `Self`.
    unsafe fn raw_clone_from(&mut self, src: *const u8);
}

impl<T: Clone + Default> Clonable for T {
    unsafe fn raw_clone_to(&self, dst: *mut u8) {
        Clone::clone_from(&mut *(dst as *mut Self), self)
    }

    unsafe fn raw_move_to(&mut self, dst: *mut u8) {
        *(dst as *mut T) = take(self);
    }

    unsafe fn raw_clone_from(&mut self, src: *const u8) {
        *self = (*(src as *const Self)).clone();
    }
}

/// Trait for trait objects whose concrete type can be cloned and moved.
// FIXME: this should be unsafe, similar to other traits in this crate.
pub trait ClonableTrait: Clonable + DowncastTrait {
    /// Clone `self` into `dst`.
    #[inline]
    fn clone_to(&self, dst: &mut Self) {
        debug_assert_eq!(self.as_any().type_id(), (dst as &Self).as_any().type_id());

        unsafe { Clonable::raw_clone_to(self, dst as *mut Self as *mut u8) }
    }

    /// Move the contents of `self` into `dst`, replacing the contents of
    /// `self` with `Self::default()`.
    #[inline]
    fn move_to(&mut self, dst: &mut Self) {
        debug_assert_eq!(
            (self as &Self).as_any().type_id(),
            (dst as &Self).as_any().type_id()
        );

        unsafe { Clonable::raw_move_to(self, dst as *mut Self as *mut u8) }
    }
}

impl<Trait> ClonableTrait for Trait where Trait: Clonable + DowncastTrait + ?Sized {}
