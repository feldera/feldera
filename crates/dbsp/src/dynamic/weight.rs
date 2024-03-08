use crate::{
    algebra::{AddAssignByRef, AddByRef, HasZero},
    declare_trait_object, declare_typed_trait_object,
    dynamic::data::DataTyped,
    DBWeight,
};

use super::{Data, DataTrait, DataTraitTyped};

/// A trait for types that can be used as weights in DBSP.
///
/// Such types must implement the plus operation and have a neutral element wrt addition (zero).
pub trait Weight: Data {
    /// Set the weight to zero.
    fn set_zero(&mut self);

    /// Check if the weight is zero.
    fn is_zero(&self) -> bool;

    /// Add the value of `rhs` to `self`, store the result in `result`.
    ///
    /// # Safety
    ///
    /// `rhs` and `result` must be valid pointers to values of type `Self`.
    unsafe fn raw_add(&self, rhs: *const u8, result: *mut u8);

    /// Add the value of `rhs` to `self`, store the result in `self`.
    ///
    /// # Safety
    ///
    /// `rhs` must be a valid pointer to a value of type `Self`
    unsafe fn raw_add_assign(&mut self, rhs: *const u8);
}

impl<T: DBWeight> Weight for T {
    fn set_zero(&mut self) {
        *self = HasZero::zero();
    }

    fn is_zero(&self) -> bool {
        HasZero::is_zero(self)
    }

    unsafe fn raw_add(&self, rhs: *const u8, result: *mut u8) {
        *(result as *mut Self) = AddByRef::add_by_ref(self, &*(rhs as *const Self))
    }

    unsafe fn raw_add_assign(&mut self, rhs: *const u8) {
        AddAssignByRef::add_assign_by_ref(self, &*(rhs as *const Self))
    }
}

/// Strongly typed weight.
///
/// A trait that includes the concrete weight type in its type signature.
/// See [`WeightTraitTyped`].
pub trait WeightTyped: Weight + DataTyped {}

impl<T> WeightTyped for T where T: Weight + DataTyped {}

/// A trait for trait objects that represent weights.
pub trait WeightTrait: DataTrait + Weight {
    /// Add `rhs` to `self` and store the result in `result`.
    fn add(&self, rhs: &Self, result: &mut Self) {
        debug_assert_eq!(self.as_any().type_id(), rhs.as_any().type_id());
        debug_assert_eq!(self.as_any().type_id(), result.as_any().type_id());

        unsafe {
            Weight::raw_add(
                self,
                rhs as *const Self as *const u8,
                result as *mut Self as *mut u8,
            )
        }
    }

    /// Add `rhs` to `self`, store the result in `self`.
    fn add_assign(&mut self, rhs: &Self) {
        debug_assert_eq!(self.as_any().type_id(), rhs.as_any().type_id());

        unsafe { Weight::raw_add_assign(self, rhs as *const Self as *const u8) }
    }
}

impl<Trait: ?Sized> WeightTrait for Trait where Trait: DataTrait + Weight {}

/// A trait for trait objects that include the concrete weight type in
/// their type signatures.
///
/// Such trait objects can be dereferenced into their concrete types.
///
/// See [`DynZWeight`](`crate::DynZWeight`) for an example of such a trait object.
pub trait WeightTraitTyped: WeightTrait + DataTraitTyped {}

impl<Trait: ?Sized> WeightTraitTyped for Trait where Trait: WeightTrait + DataTraitTyped {}

declare_trait_object!(DynWeight<> = dyn Weight<>
where
);

declare_typed_trait_object!(DynWeightTyped<T> = dyn WeightTyped<Type = T>
    [T]
where
    T: DBWeight,
);
