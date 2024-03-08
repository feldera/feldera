use std::mem::take;

use crate::{declare_trait_object, dynamic::erase::Erase, DBData};

use super::{Data, DataTrait};

/// An dynamically typed interface to `Option<>`.
///
/// The type argument `T` is a trait object type that represents
/// the inner value of the `Option` type.
pub trait Opt<T: DataTrait + ?Sized>: Data {
    /// Return a reference to the inner value or `None` if the value is not set.
    fn get(&self) -> Option<&T>;

    /// Return a mutable reference to the inner value of `None` if the value is not set.
    fn get_mut(&mut self) -> Option<&mut T>;

    /// Set the value to `None`.
    fn set_none(&mut self);

    /// Clone the value of `v` into `self`.
    #[allow(clippy::wrong_self_convention)]
    fn from_ref(&mut self, v: &T);

    /// Move the value of `v` into `self` without cloning.  Clears the contents of `v`,
    /// i.e., sets it to the default value for the type.
    #[allow(clippy::wrong_self_convention)]
    fn from_val(&mut self, v: &mut T);

    /// Put a fresh value in `self` and call `f` to initialize it.   
    fn set_some_with(&mut self, f: &mut dyn FnMut(&mut T));
}

impl<T, Trait> Opt<Trait> for Option<T>
where
    Trait: DataTrait + ?Sized,
    T: DBData + Erase<Trait>,
{
    fn get(&self) -> Option<&Trait> {
        self.as_ref().map(Erase::erase)
    }

    fn get_mut(&mut self) -> Option<&mut Trait> {
        self.as_mut().map(Erase::erase_mut)
    }

    fn set_none(&mut self) {
        *self = None;
    }

    fn from_ref(&mut self, v: &Trait) {
        *self = Some(unsafe { v.downcast::<T>().clone() })
    }

    fn from_val(&mut self, v: &mut Trait) {
        *self = Some(unsafe { take(v.downcast_mut::<T>()) })
    }

    fn set_some_with(&mut self, f: &mut dyn FnMut(&mut Trait)) {
        let mut v: T = Default::default();
        f(v.erase_mut());
        *self = Some(v);
    }
}

declare_trait_object!(DynOpt<T> = dyn Opt<T>
where
    T: DataTrait + ?Sized
);
