#[cfg(doc)]
use crate::zset_set;

/// Create an indexed Z-set with specified elements.
///
/// This macro is used in unit tests to create reference inputs and outputs.
/// It generates an indexed Z-set of type
/// [`OrdIndexedZSet`](crate::OrdIndexedZSet)s.
///
/// # Example
///
/// The following example shows how to construct an indexed Z-set with `(key,
/// value, weight)` tuples `(1, 3, -1), (1, 2, 1), (2, 2, 1)`:
///
/// ```
/// use dbsp::{indexed_zset, IndexedZSet};
///
/// let zset = indexed_zset! {1 => {3 => -1, 2 => 1}, 2 => {2 => 1}};
/// assert_eq!(
///     zset.iter().collect::<Vec<_>>(),
///     vec![(1, 2, 1), (1, 3, -1), (2, 2, 1)]
/// );
/// ```
#[macro_export]
macro_rules! indexed_zset {
    ($key_type:ty => $val_type:ty: $($key:expr => { $($value:expr => $weight:expr),* }),* $(,)?) => {{
        let batch = ::std::vec![ $( $( $crate::utils::Tup2($crate::utils::Tup2($key, $value), $weight) ),* ),* ];

        $crate::typed_batch::OrdIndexedZSet::<$key_type, $val_type>::from_tuples((), batch)
    }};

    ($($key:expr => { $($value:expr => $weight:expr),* }),* $(,)?) => {{
        let batch = ::std::vec![ $( $( $crate::utils::Tup2($crate::utils::Tup2($key, $value), $weight) ),* ),* ];

        $crate::typed_batch::OrdIndexedZSet::from_tuples((), batch)
    }};
}

/// Create a Z-set with specified elements.
///
/// This macro is used in unit tests to create reference inputs and outputs.
/// It generates a Z-set of type [`OrdZSet`](crate::OrdZSet)s.
///
/// If all the elements in the Z-set will have weight 1, consider [`zset_set!`].
///
/// # Example
///
/// The following example shows how to construct a Z-set with `(key, weight)`
/// tuples `(1, -1), (2, 1)`:
///
/// ```
/// use dbsp::{zset, IndexedZSet};
///
/// let zset = zset! {1 => -1, 2 => 1};
/// assert_eq!(
///     zset.iter().collect::<Vec<_>>(),
///     vec![(1, (), -1), (2, (), 1)]
/// );
/// ```
#[macro_export]
macro_rules! zset {
    ( $( $key:expr => $weight:expr ),* $(,)?) => {{
        let batch = ::std::vec![ $( $crate::utils::Tup2($crate::utils::Tup2($key, ()), $weight) ),* ];

        $crate::typed_batch::OrdZSet::from_tuples((), batch)
    }};

}

/// Create a Z-set with specified elements all with weight 1.
///
/// This macro is used in unit tests to create reference inputs and outputs.
/// It generates a Z-set of type [`OrdZSet`](crate::OrdZSet)s.
///
/// # Example
///
/// The following examples shows how to construct a Z-set with `(key, weight)`
/// tuples `(1, 1), (2, 1), (-3, 1)`:
///
/// ```
/// use dbsp::{zset_set, IndexedZSet};
///
/// let zset = zset_set! {1, 2, -3};
/// assert_eq!(
///     zset.iter().collect::<Vec<_>>(),
///     vec![(-3, (), 1), (1, (), 1), (2, (), 1)]
/// );
/// ```
#[macro_export]
macro_rules! zset_set {
    ( $( $key:expr ),* $(,)?) => {{
        let batch = ::std::vec![ $( $crate::utils::Tup2($crate::utils::Tup2($key, ()), 1) ),* ];

        $crate::typed_batch::OrdZSet::from_tuples((), batch)
    }};
}

#[cfg(test)]
mod test {
    use crate::{indexed_zset, zset};

    #[test]
    fn zset_test() {
        let zset = zset! {1 => -1, 2 => 1};
        assert_eq!(
            zset.iter().collect::<Vec<_>>(),
            vec![(1, (), -1), (2, (), 1)]
        );
    }

    #[test]
    fn indexed_zset_test() {
        let zset = indexed_zset! { u64 => i64: 1 => {3 => -1, 2 => 1}, 2 => {2 => 1}};
        assert_eq!(
            zset.iter().collect::<Vec<_>>(),
            vec![(1, 2, 1), (1, 3, -1), (2, 2, 1)]
        );
    }

    #[test]
    fn zset_set_test() {
        let zset = zset_set! {1i32, 2, -3};
        assert_eq!(
            zset.iter().collect::<Vec<_>>(),
            vec![(-3i32, (), 1), (1, (), 1), (2, (), 1)]
        );
    }
}
