//! This module contains type and trait declarations that support the DBSP dynamic dispatch
//! architecture.
//!
//! ## Background
//!
//! DBSP operators manipulate batches of records that represent keys, values, and weights
//! in various collections. Each record is represented as a concrete Rust type, e.g., a struct
//! or a tuple.  Operators are parameterized by the types of their input and output batches,
//! which are in turn parameterized by the types of keys, values, and weights in the batch.
//! This means that, had we used static typing (which is the default in Rust), the Rust
//! compiler would generate a specialized implementation of each operator for each combination
//! of input and output types that occur in the program.  In an earlier version of this
//! library, this approach led to very long compilation times for even medium-sized programs.
//!
//! The solution we adopt relies on dynamic dispatch instead.  We bundle primitive operations
//! on types (comparison, cloning, etc.) as object-safe traits and expose them to operators
//! as trait objects.  This speeds up compilation without sacrificing much performance.
//!
//! Implementing this approach involves some machinery.  First, trait objects cannot be
//! efficiently combined into any kind of containers (vectors, sets, or even tuples or structs)
//! without wrapping each object in a `Box` or some other heap-allocated type, leading to many
//! small allocations.  To avoid this, we represent such containers as separate trait objects.
//! For instance, options (`Option<T>`), 2-tuples (`(T1, T2)`), vectors (`Vec<T>`), and sets
//! (`Set<T>`) are modeled as traits.
//!
//! Second, this design introduces unsafe code that cannot be nicely encapsulated.  By
//! replacing concrete types with trait objects, we lose the ability to distinguish between trait
//! objects backed by different types.  Operations like comparison and cloning must downcast
//! their arguments to the same concrete type.  This can be done safely by checking the
//! [`TypeId`](core::any::TypeId)s
//! of the arguments, but this check is expensive in Rust when performed on the critical
//! path.  We therefore elide this check in release builds, making all such operations unsafe.
//!
//! Finally, operators require the ability to create new values, which in turn requires passing
//! a **factory** object for each type the operator may need to instantiate.  Most operators
//! (as well as low-level data structures they use internally like batches and traces) require
//! multiple factory objects.
//!
//! ## Dynamic API design considerations
//!
//! Since trait objects don't have a statically known size, they cannot be returned by value
//! (without boxing them, which we don't want to do in most cases).  We therefore pass mutable
//! references for functions to write return values to.  This requires the caller to allocate
//! space for the return value using a factory object.
//!
//! Likewise, trait objects cannot be passed by value; therefore functions that would normally
//! take an owned value instead take a mutable reference, which they are allowed to use without
//! cloning, e.g., with [`ClonableTrait::move_to`](`crate::dynamic::ClonableTrait::move_to`), which
//! moves the value, leaving the default value in its place.
//!
//! ## Trait hierarchy
//!
//! We encapsulate common operations in a hierarchy of object-safe traits, i.e., traits for which
//! the compiler can build vtables and expose them as trait objects (`dyn <Trait>`).
//!
//! ### Basic traits
//!
//! The following traits must be implemented for all values that DBSP can compute on.
//! All of these traits include blanket implementation for all the types that can support them:
//!
//! * [`AsAny`] - convert a reference to a type to `&dyn Any`.
//! * [`Clonable`] - an object-safe version of `Clone`.
//! * [`Comparable`] - an object-safe version of `Ord`.
//! * [`SerializeDyn`] - an object-safe version of `rkyv::Serialize`.
//! * [`DeserializableDyn`] - an object-safe version of `rkyv::Deserialize`.
//! * [`Data`] - the lowest common denominator for all types that DBSP can compute on.
//!   Combines all of the above traits, along with `Debug`, `SizeOf`, and `Send`.  This
//!   is an object-safe version of [`crate::DBData`].
//!
//! In addition, the [`Erase`] trait is provided to convert concrete types into trait objects.
//!
//! ### Trait object traits
//!
//! The following traits are meant to be implemented by **trait objects** rather than regular
//! types, i.e., an instance of one of these traits is a `dyn Trait` (which is a type in Rust).
//! The reason we need these traits is ergonomics, since not all useful behaviors can be
//! captured nicely in an object-safe way.  For instance `Eq`, `Ord`, and `Clone` traits
//! are not object-safe, so we cannot expose them directly via the vtable.  We instead introduce
//! [`Comparable`] and [`Clonable`] traits mentioned above.  These traits define an unsafe
//! API that accepts one of the arguments as a raw byte pointer (`*mut u8`).  These are not
//! meant to be used directly.  Instead, we use them to implement `Eq`, `Ord`, and `Clone` for
//! trait objects.
//!
//! * [`DowncastTrait`] - cast a trait object reference to a concrete type.
//! * [`ClonableTrait`] - trait for trait objects whose concrete type can be cloned and moved.
//! * [`ArchiveTrait`] - trait for trait objects that can be serialized and deserialized with `rkyv`.
//! * [`DataTrait`] - the lowest common denominator for all trait objects that DBSP can compute on.
//!   Combines all of the above traits, along with [`Data`].  This
//!   is the trait object version of [`crate::DBData`].
//!
//! ### Creating trait objects
//!
//! There are three ways to create a trait object:
//!
//! * The [`Factory`] trait can be used to allocate an instance of a concrete type with the default
//!   value on the heap or on the stack and return a reference to it as a trait object.
//!
//! * The [`Erase`] trait converts a reference to a concrete type into a trait object.
//!
//! * [`DeserializableDyn`] and [`DeserializeDyn`] deserialize values of the concrete type from
//!   a byte array or from an archived representation respectively and returns them as trait
//!   objects.
//!
//! ### Derived traits
//!
//! The traits described above are applicable to all DBSP values.  The following traits specify
//! extended functionality available for certain types and their combinations.
//!
//! * [`DynWeight`] ([`WeightTrait`]) - types with the addition operation and a zero element.
//!   This is an object-safe version of [`crate::DBWeight`].
//! * [`DynOpt`] - a dynamically typed version of the `Option` type.
//! * [`DynPair`] - a dynamically typed version of a two-tuple `(T1, T2)`.
//! * [`DynVec`] - a dynamically typed vector.
//! * [`DynSet`] - a dynamically typed B-tree set.
//! * [`DynPairs`] - a vector of pairs.
//! * [`DynWeightedPairs`] - a vector of key-value pairs, where the value behaves as weight,
//!   meaning that tuples with the same key can be consolidated by adding their weights.

mod clonable;
mod comparable;
pub(crate) mod data;
mod declare_trait_object;
mod downcast;
mod erase;
mod factory;
mod lean_vec;
mod option;
pub mod pair;
mod pairs;
pub(crate) mod rkyv;
mod set;
mod vec;
mod weight;
mod weighted_pairs;

pub use clonable::{Clonable, ClonableTrait};
pub use comparable::Comparable;
pub use data::{Data, DataTrait, DataTraitTyped, DynBool, DynData, DynDataTyped, DynUnit};
pub use downcast::{AsAny, DowncastTrait};
pub use erase::Erase;
pub use factory::{Factory, WithFactory};
pub use lean_vec::{LeanVec, RawIter};
pub use option::{DynOpt, Opt};
pub use pair::{DynPair, Pair};
pub use pairs::{DynPairs, Pairs, PairsTrait};
pub use rkyv::{
    ArchiveTrait, ArchivedDBData, DeserializableDyn, DeserializeDyn, DeserializeImpl, SerializeDyn,
};
pub use set::{BSet, DynSet, Set, SetTrait};
pub use vec::{DynVec, VecTrait, Vector};
pub use weight::{DynWeight, DynWeightTyped, Weight, WeightTrait, WeightTraitTyped};
pub use weighted_pairs::{DynWeightedPairs, WeightedPairs, WeightedPairsTrait};
