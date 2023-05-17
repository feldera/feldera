#[cfg(doc)]
use crate::zset_set;

/// Create an indexed Z-set with specified elements.
///
/// This macro is used in unit tests to create reference inputs and outputs.
/// It generates an indexed Z-set of type
/// [`OrdIndexedZSet`](crate::trace::ord::OrdIndexedZSet)s.
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
    ( $($key:expr => { $($value:expr => $weight:expr),* }),* $(,)?) => {{
        let mut batcher = <<$crate::trace::ord::OrdIndexedZSet<_, _, _> as $crate::trace::Batch>::Batcher as $crate::trace::Batcher<_, _, _, _>>::new_batcher(());
        let mut batch = ::std::vec![ $( $( (($key, $value), $weight) ),* ),* ];
        $crate::trace::Batcher::push_batch(&mut batcher, &mut batch);
        $crate::trace::Batcher::seal(batcher)
    }};
}

/// Create a Z-set with specified elements.
///
/// This macro is used in unit tests to create reference inputs and outputs.
/// It generates a Z-set of type [`OrdZSet`](crate::trace::ord::OrdZSet)s.
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
        let mut batcher = <<$crate::trace::ord::OrdZSet<_, _> as $crate::trace::Batch>::Batcher as $crate::trace::Batcher<_, _, _, _>>::new_batcher(());

        let mut batch = ::std::vec![ $( ($key, $weight) ),* ];
        $crate::trace::Batcher::push_batch(&mut batcher, &mut batch);
        $crate::trace::Batcher::seal(batcher)
    }};
}

/// Create a Z-set with specified elements all with weight 1.
///
/// This macro is used in unit tests to create reference inputs and outputs.
/// It generates a Z-set of type [`OrdZSet`](crate::trace::ord::OrdZSet)s.
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
        let mut batcher = <<$crate::trace::ord::OrdZSet<_, _> as $crate::trace::Batch>::Batcher as $crate::trace::Batcher<_, _, _, _>>::new_batcher(());

        let mut batch = ::std::vec![ $( ($key, 1) ),* ];
        $crate::trace::Batcher::push_batch(&mut batcher, &mut batch);
        $crate::trace::Batcher::seal(batcher)
    }};
}
