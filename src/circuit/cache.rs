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
    ($constructor:ident<$($typearg:ident),*>($key_type:ty => $val_type:ty)) => {
        #[repr(transparent)]
        pub struct $constructor<$($typearg: 'static),*>(pub $key_type, std::marker::PhantomData<($($typearg),*)>);

        impl<$($typearg),*> $constructor<$($typearg),*> {
            pub fn new(key: $key_type) -> Self {
                Self(key, std::marker::PhantomData)
            }
        }

        impl<$($typearg),*> std::hash::Hash for $constructor<$($typearg),*> {
            fn hash<H>(&self, state: &mut H)
            where
                H: std::hash::Hasher
            {
                self.0.hash(state);
            }
        }

        impl<$($typearg),*> PartialEq for $constructor<$($typearg),*> {
            fn eq(&self, other: &Self) -> bool {
                self.0.eq(&other.0)
            }
        }

        impl<$($typearg),*> Eq for $constructor<$($typearg),*> {}

        impl<$($typearg: 'static),*> typedmap::TypedMapKey<$crate::circuit::cache::CircuitStoreMarker> for $constructor<$($typearg),*> {
            type Value = $val_type;
        }
    }
}
