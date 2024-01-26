use crate::{
    trace::{
        layers::{
            file::column_layer::FileColumnLayer, Builder, Cursor, MergeBuilder, Trie, TupleBuilder,
        },
        ord::file::StorageBackend,
    },
    DBData, DBWeight,
};
use feldera_storage::file::{
    reader::Reader,
    writer::{Parameters, Writer1},
};
use size_of::SizeOf;
use std::cmp::Ordering;

/// Implements [`MergeBuilder`] and [`TupleBuilder`] for [`FileColumnLayer`].
pub struct FileColumnLayerBuilder<K, R>(Writer1<StorageBackend, K, R>)
where
    K: DBData,
    R: DBWeight;

impl<K, R> Builder for FileColumnLayerBuilder<K, R>
where
    K: DBData,
    R: DBWeight,
{
    type Trie = FileColumnLayer<K, R>;

    fn boundary(&mut self) -> usize {
        self.keys()
    }

    fn done(self) -> Self::Trie {
        FileColumnLayer {
            file: Reader::new(
                &StorageBackend::default_for_thread(),
                self.0.close().unwrap(),
            )
            .unwrap(),
            lower_bound: 0,
        }
    }
}

impl<K, R> MergeBuilder for FileColumnLayerBuilder<K, R>
where
    K: DBData,
    R: DBWeight,
{
    fn with_capacity(left: &Self::Trie, right: &Self::Trie) -> Self {
        let capacity = Trie::keys(left) + Trie::keys(right);
        Self::with_key_capacity(capacity)
    }

    fn with_key_capacity(_capacity: usize) -> Self {
        Self(Writer1::new(&StorageBackend::default_for_thread(), Parameters::default()).unwrap())
    }

    fn reserve(&mut self, _additional: usize) {}

    fn keys(&self) -> usize {
        self.0.n_rows() as usize
    }

    fn copy_range_retain_keys<'a, F>(
        &mut self,
        other: &'a Self::Trie,
        lower: usize,
        upper: usize,
        filter: &F,
    ) where
        F: Fn(&<<Self::Trie as Trie>::Cursor<'a> as Cursor<'a>>::Key) -> bool,
    {
        let mut cursor = other.cursor_from(lower, upper);
        while cursor.valid() {
            let item = cursor.current_item();
            if filter(&item.0) {
                self.0.write0((&item.0, &item.1)).unwrap();
            }
            cursor.step();
        }
    }

    fn push_merge_retain_keys<'a, F>(
        &'a mut self,
        mut cursor1: <Self::Trie as Trie>::Cursor<'a>,
        mut cursor2: <Self::Trie as Trie>::Cursor<'a>,
        filter: &F,
    ) where
        F: Fn(&<<Self::Trie as Trie>::Cursor<'a> as Cursor<'a>>::Key) -> bool,
    {
        while cursor1.valid() && cursor2.valid() {
            match cursor1.current_key().cmp(cursor2.current_key()) {
                Ordering::Less => {
                    let item = cursor1.current_item();
                    if filter(&item.0) {
                        self.0.write0((&item.0, &item.1)).unwrap();
                    }
                    cursor1.step();
                }
                Ordering::Equal => {
                    let mut sum = cursor1.current_diff().clone();
                    sum.add_assign_by_ref(cursor2.current_diff());

                    if !sum.is_zero() && filter(cursor1.current_key()) {
                        self.0.write0(((cursor1.current_key()), &sum)).unwrap();
                    }
                    cursor1.step();
                    cursor2.step();
                }

                Ordering::Greater => {
                    let item = cursor2.current_item();
                    if filter(&item.0) {
                        self.0.write0((&item.0, &item.1)).unwrap();
                    }
                    cursor2.step();
                }
            }
        }

        while let Some(item) = cursor1.take_current_item() {
            if filter(&item.0) {
                self.0.write0((&item.0, &item.1)).unwrap();
            }
        }
        while let Some(item) = cursor2.take_current_item() {
            if filter(&item.0) {
                self.0.write0((&item.0, &item.1)).unwrap();
            }
        }
    }
}

impl<K, R> TupleBuilder for FileColumnLayerBuilder<K, R>
where
    K: DBData,
    R: DBWeight,
{
    type Item = (K, R);

    fn new() -> Self {
        Self(Writer1::new(&StorageBackend::default_for_thread(), Parameters::default()).unwrap())
    }

    fn with_capacity(_capacity: usize) -> Self {
        Self::new()
    }

    fn reserve_tuples(&mut self, _additional: usize) {}

    fn tuples(&self) -> usize {
        self.0.n_rows() as usize
    }

    fn push_tuple(&mut self, item: (K, R)) {
        self.0.write0((&item.0, &item.1)).unwrap();
    }
}

impl<K, R> SizeOf for FileColumnLayerBuilder<K, R>
where
    K: DBData,
    R: DBWeight,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
    }
}
