//! Compile-time classification of types for the inline sparse tuple format.
//!
//! Types whose `rkyv::Archived` form has a fixed, known size implement
//! [`ArchiveLayout`] with `IS_FIXED = true`. Variable-size types (String, Vec,
//! etc.) set `IS_FIXED = false` and are stored via an `i32` relative pointer
//! (same encoding as `rkyv::rel_ptr::RawRelPtrI32`) in the inline sparse
//! tuple format.

use uuid::Uuid;

/// Maximum alignment used in the inline sparse tuple format.
/// The blob start is padded to this alignment so that all field
/// alignments (which must divide this value) are satisfied.
pub const INLINE_SPARSE_MAX_ALIGN: usize = 16;

/// Compile-time classification of a type's archived layout.
///
/// Used by the inline sparse tuple format to decide whether a field's archived
/// bytes are stored directly inline (fixed-size) or via an `i32` relative
/// pointer to elsewhere in the rkyv buffer (variable-size).
///
/// Implement this for the *inner* type (i.e., `<T as IsNone>::Inner`).
pub trait ArchiveLayout {
    /// `true` if `rkyv::Archived<Self>` has a fixed, known size.
    const IS_FIXED: bool;

    /// Size in bytes of the archived form.  Only meaningful when `IS_FIXED` is
    /// `true`; when `IS_FIXED` is `false` this should be `0`.
    const ARCHIVED_SIZE: usize;

    /// Alignment required for this field in the inline sparse tuple body.
    /// For fixed-size types this is the alignment of `Archived<Self>`.
    /// For variable-size types this is 1 (the i32 relative pointer is read
    /// via `read_unaligned`).
    const INLINE_ALIGN: usize;

    /// Number of bytes this field occupies in the inline body of an inline
    /// sparse tuple: either [`ARCHIVED_SIZE`](Self::ARCHIVED_SIZE) for
    /// fixed-size types or 4 (size of an `i32` relative pointer) for
    /// variable-size types.
    const INLINE_SIZE: usize = if Self::IS_FIXED {
        Self::ARCHIVED_SIZE
    } else {
        4 // i32 relative pointer
    };
}

// ---------------------------------------------------------------------------
// Fixed-size implementations for primitive types
// ---------------------------------------------------------------------------

macro_rules! fixed_layout {
    ($($ty:ty => ($size:expr, $align:expr)),* $(,)?) => {
        $(
            impl ArchiveLayout for $ty {
                const IS_FIXED: bool = true;
                const ARCHIVED_SIZE: usize = $size;
                const INLINE_ALIGN: usize = $align;
            }
        )*
    };
}

fixed_layout! {
    bool  => (1, 1),
    char  => (4, 4), // Archived<char> = u32_le
    i8    => (1, 1),
    u8    => (1, 1),
    i16   => (2, 2),
    u16   => (2, 2),
    i32   => (4, 4),
    u32   => (4, 4),
    f32   => (4, 4),
    i64   => (8, 8),
    u64   => (8, 8),
    f64   => (8, 8),
    isize => (8, 8),
    usize => (8, 8),
    i128  => (16, 16),
    u128  => (16, 16),
    ()    => (0, 1),
    Uuid  => (16, 1),
}

// ---------------------------------------------------------------------------
// Variable-size implementations
// ---------------------------------------------------------------------------

macro_rules! var_layout {
    ($($ty:ty),* $(,)?) => {
        $(
            impl ArchiveLayout for $ty {
                const IS_FIXED: bool = false;
                const ARCHIVED_SIZE: usize = 0;
                const INLINE_ALIGN: usize = 1;
            }
        )*
    };
}

var_layout!(String);

impl<T> ArchiveLayout for Vec<T> {
    const IS_FIXED: bool = false;
    const ARCHIVED_SIZE: usize = 0;
    const INLINE_ALIGN: usize = 1;
}

impl<T> ArchiveLayout for std::sync::Arc<T> {
    const IS_FIXED: bool = false;
    const ARCHIVED_SIZE: usize = 0;
    const INLINE_ALIGN: usize = 1;
}

impl<K, V> ArchiveLayout for std::collections::BTreeMap<K, V> {
    const IS_FIXED: bool = false;
    const ARCHIVED_SIZE: usize = 0;
    const INLINE_ALIGN: usize = 1;
}
