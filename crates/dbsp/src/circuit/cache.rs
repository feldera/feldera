//! Circuit cache.
//!
//! # Background
//!
//! We use a form of caching to reuse previously constructed fragments of the
//! circuit. For instance, a
//! [`stream.integrate()`](`crate::circuit::Stream::integrate`) call creates a
//! feedback loop with an adder and a `z^-1` operator.  The output of this
//! circuit is cached, so that the next identical call will return the same
//! stream without creating a new copy of the same operators.  Stream integral
//! is one example of a derived stream.  Others include
//! [nested integral](`crate::circuit::Stream::integrate_nested`), [delayed
//! stream](`crate::circuit::Stream::delay`), etc.  Caching allows simplifying
//! high-level APIs like nested incremental join that combine several derived
//! streams.  Without caching, such methods would have to either construct all
//! derived streams internally, potentially creating duplicate operators and
//! thus wasting CPU and memory or take multiple derived streams as input, which
//! would require the caller to track these derivatives manually.
//!
//! # API
//!
//! This module provides a low-level mechanism that individual operators use to
//! perform operator-specific caching.  The mechanism consists of a key-value
//! store associated with each circuit.  We use `TypedMap`, which supports
//! multiple key and value types, for the store.  An operator registers a new
//! key type and associated value type using the `circuit_cache_key` macro,
//! e.g.,
//!
//! ```ignore
//! circuit_cache_key!(IntegralId<C, D>(NodeId => Stream<C, D>));
//! ```
//!
//! declares a mapping from `NodeId` to `Stream` used by the
//! [`Stream::integrate`](`crate::circuit::Stream::integrate`) method to cache
//! the output of the integrator.  Internally, the method first performs a map
//! lookup and returns the cached stream, if one exists; otherwise it
//! instantiates the integration circuit and stores its output stream in the
//! cache before returning it to the caller.

use typedmap::TypedMap;

pub struct CircuitStoreMarker;

/// Per-circuit cache.
pub type CircuitCache = TypedMap<CircuitStoreMarker>;

/// Declare an anonymous struct type to be used as a key in the cache and
/// associated value type.
///
/// # Example
///
/// ```ignore
/// circuit_cache_key!(IntegralId<C, D>(NodeId => Stream<C, D>));
/// ```
///
/// declares `struct IntegralId(NodeId)` key type with associated value type
/// `Stream<C, D>`.
#[macro_export]
macro_rules! circuit_cache_key {
    ($constructor:ident$(<$($typearg:ident $(:$trait_bound:tt)? ),*>)?($key_type:ty => $val_type:ty)) => {
        circuit_cache_key!(@inner pub [$crate::circuit::cache::CircuitStoreMarker] $constructor $(<$($typearg $(:$trait_bound)?),*>)?($key_type => $val_type));
    };

    (local $vis:vis $constructor:ident$(<$($typearg:ident $(:$trait_bound:tt)?),*>)?($key_type:ty => $val_type:ty)) => {
        circuit_cache_key!(@inner $vis [$crate::circuit::runtime::LocalStoreMarker] $constructor $(<$($typearg $(:$trait_bound)?),*>)?($key_type => $val_type));
    };

    (@inner $vis:vis [$type_key:path] $constructor:ident$(<$($typearg:ident $(:$trait_bound:tt)?),*>)?($key_type:ty => $val_type:ty)) => {
        #[repr(transparent)]
        #[allow(unused_parens)]
        $vis struct $constructor$(<$($typearg: 'static),*>)?(pub $key_type, $(::std::marker::PhantomData<($($typearg),*)>)?);

        impl$(<$($typearg),*>)? $constructor$(<$($typearg),*>)? {
            #[allow(unused_parens)]
            pub fn new(key: $key_type) -> Self {
                Self(key, $(::std::marker::PhantomData::<($($typearg),*)>)?)
            }
        }

        impl$(<$($typearg),*>)? ::std::hash::Hash for $constructor$(<$($typearg),*>)? {
            fn hash<H>(&self, state: &mut H)
            where
                H: ::std::hash::Hasher,
            {
                ::std::hash::Hash::hash(&self.0, state);
            }
        }

        impl$(<$($typearg),*>)? ::std::cmp::PartialEq for $constructor$(<$($typearg),*>)? {
            fn eq(&self, other: &Self) -> bool {
                ::std::cmp::PartialEq::eq(&self.0, &other.0)
            }
        }

        impl$(<$($typearg),*>)? ::std::cmp::Eq for $constructor$(<$($typearg),*>)? {}

        impl$(<$($typearg $(:$trait_bound)?),*>)? ::typedmap::TypedMapKey<$type_key> for $constructor$(<$($typearg),*>)? {
            type Value = $val_type;
        }

        unsafe impl$(<$($typearg: 'static),*>)? Send for $constructor$(<$($typearg),*>)? {}
        unsafe impl$(<$($typearg: 'static),*>)? Sync for $constructor$(<$($typearg),*>)? {}
    };
}

#[macro_export]
macro_rules! circuit_cache_key_unsized {
    ($constructor:ident$(<$($typearg:ident),*>)?($key_type:ty => $val_type:ty)) => {
        circuit_cache_key_unsized!(@inner pub [$crate::circuit::cache::CircuitStoreMarker] $constructor $(<$($typearg),*>)?($key_type => $val_type));
    };

    (local $vis:vis $constructor:ident$(<$($typearg:ident),*>)?($key_type:ty => $val_type:ty)) => {
        circuit_cache_key_unsized!(@inner $vis [$crate::circuit::runtime::LocalStoreMarker] $constructor $(<$($typearg),*>)?($key_type => $val_type));
    };

    (@inner $vis:vis [$type_key:path] $constructor:ident$(<$($typearg:ident),*>)?($key_type:ty => $val_type:ty)) => {
        #[repr(transparent)]
        #[allow(unused_parens)]
        $vis struct $constructor$(<$($typearg: 'static + ?Sized),*>)?(pub $key_type, $(::std::marker::PhantomData<fn($(& $typearg),*)>)?);

        impl$(<$($typearg: ?Sized),*>)? $constructor$(<$($typearg),*>)? {
            #[allow(unused_parens)]
            pub fn new(key: $key_type) -> Self {
                Self(key, $(::std::marker::PhantomData::<($($typearg),*)>)?)
            }
        }

        impl$(<$($typearg: ?Sized),*>)? ::std::hash::Hash for $constructor$(<$($typearg),*>)? {
            fn hash<H>(&self, state: &mut H)
            where
                H: ::std::hash::Hasher,
            {
                ::std::hash::Hash::hash(&self.0, state);
            }
        }

        impl$(<$($typearg: ?Sized),*>)? ::std::cmp::PartialEq for $constructor$(<$($typearg),*>)? {
            fn eq(&self, other: &Self) -> bool {
                ::std::cmp::PartialEq::eq(&self.0, &other.0)
            }
        }

        impl$(<$($typearg: ?Sized),*>)? ::std::cmp::Eq for $constructor$(<$($typearg),*>)? {}

        impl$(<$($typearg: ?Sized + 'static),*>)? ::typedmap::TypedMapKey<$type_key> for $constructor$(<$($typearg),*>)? {
            type Value = $val_type;
        }

        unsafe impl$(<$($typearg: ?Sized + 'static),*>)? Send for $constructor$(<$($typearg),*>)? {}
        unsafe impl$(<$($typearg: ?Sized + 'static),*>)? Sync for $constructor$(<$($typearg),*>)? {}
    };
}
