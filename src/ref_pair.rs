//! Trait for types that can be converted into a pair of references.

/// Trait for types that can be converted into a pair of references
///
/// This trait unifies `(&K, &V)` and `&(K, V)` types, so that
/// operator can work on collection types that can iterate over either,
/// such as vectors (which iterate over `&(K,V)`) and maps (which iterate
/// over `(&K, &V)`)
pub trait RefPair<'a, K, V> {
    fn into_refs(self) -> (&'a K, &'a V);
}

impl<'a, K, V> RefPair<'a, K, V> for &'a (K, V) {
    fn into_refs(self) -> (&'a K, &'a V) {
        (&self.0, &self.1)
    }
}

impl<'a, K, V> RefPair<'a, K, V> for (&'a K, &'a V) {
    fn into_refs(self) -> (&'a K, &'a V) {
        (self.0, self.1)
    }
}
