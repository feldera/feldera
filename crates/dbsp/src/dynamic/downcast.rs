use std::any::{Any, TypeId};

/// Trait for types that return a reference to self as `dyn Any`.
///
/// Used to construct [`DowncastTrait`].
pub trait AsAny: 'static {
    fn as_any(&self) -> &dyn Any;
}

impl<T: 'static> AsAny for T {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// A trait for trait objects that can be downcast to a reference
/// to the concrete type they wrap.
// NOTE: We don't auto-derive this trait for all types that satisfy
// the bound.  Instead it needs to be derived manually for trait
// object types only.
pub trait DowncastTrait: AsAny {
    /// Cast trait object reference to a reference to a concrete type `T`.
    ///
    /// Panics if the inner value of `self` does not have type `T`.
    #[inline]
    fn downcast_checked<T: AsAny>(&self) -> &T {
        assert_eq!(
            self.as_any().type_id(),
            TypeId::of::<T>(),
            "downcast_checked from incorrect type to {}",
            std::any::type_name::<T>()
        );
        unsafe { &*(self as *const _ as *const T) }
    }

    /// Cast trait object reference to a mutable reference to a concrete type `T`.
    ///
    /// Panics if the inner value of `self` does not have type `T`.
    #[inline]
    fn downcast_mut_checked<T: AsAny>(&mut self) -> &mut T {
        assert_eq!(
            (self as &Self).as_any().type_id(),
            TypeId::of::<T>(),
            "downcast_mut_checked from incorrect type to {}",
            std::any::type_name::<T>()
        );
        unsafe { &mut *(self as *mut _ as *mut T) }
    }

    /// Cast trait object reference to a reference to a concrete type `T`, eliding type checking.
    ///
    /// # Safety
    ///
    /// The inner value of `self` must be of type `T`.
    #[inline]
    unsafe fn downcast<T: AsAny>(&self) -> &T {
        debug_assert_eq!(
            self.as_any().type_id(),
            TypeId::of::<T>(),
            "downcast from incorrect type to {}",
            std::any::type_name::<T>()
        );
        &*(self as *const _ as *const T)
    }

    /// Cast trait object reference to a reference to a mutable reference to concrete type `T`,
    /// eliding type checking.
    ///
    /// # Safety
    ///
    /// The inner value of `self` must be of type `T`.
    #[inline]
    unsafe fn downcast_mut<T: AsAny>(&mut self) -> &mut T {
        debug_assert_eq!(
            (self as &Self).as_any().type_id(),
            TypeId::of::<T>(),
            "downcast_mut from incorrect type to {}",
            std::any::type_name::<T>()
        );
        &mut *(self as *mut _ as *mut T)
    }
}
