use crate::{
    dynamic::{DataTrait, WeightTrait},
    storage::file::writer::{Parameters, Writer1},
    trace::{
        layers::{
            file::column_layer::{FileColumnLayer, FileLeafFactories},
            Builder, Cursor, MergeBuilder, Trie, TupleBuilder,
        },
        ord::file::StorageBackend,
    },
    Runtime,
};
use size_of::SizeOf;
use std::cmp::Ordering;

/// Implements [`MergeBuilder`] and [`TupleBuilder`] for [`FileColumnLayer`].
pub struct FileColumnLayerBuilder<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    factories: FileLeafFactories<K, R>,
    writer: Writer1<StorageBackend, K, R>,
}

impl<K, R> Builder for FileColumnLayerBuilder<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Trie = FileColumnLayer<K, R>;

    fn boundary(&mut self) -> usize {
        self.keys()
    }

    fn done(self) -> Self::Trie {
        FileColumnLayer {
            factories: self.factories.clone(),
            file: self.writer.into_reader().unwrap(),
            lower_bound: 0,
        }
    }
}

impl<K, R> MergeBuilder for FileColumnLayerBuilder<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn with_capacity(left: &Self::Trie, _right: &Self::Trie) -> Self {
        Self {
            factories: left.factories.clone(),
            writer: Writer1::new(
                &left.factories.file_factories,
                &Runtime::storage(),
                Parameters::default(),
            )
            .unwrap(),
        }
    }

    fn reserve(&mut self, _additional: usize) {}

    fn keys(&self) -> usize {
        self.writer.n_rows() as usize
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
            if filter(item.0) {
                self.writer.write0((&item.0, &item.1)).unwrap();
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
        let mut sum = self.factories.diff_factory.default_box();

        while cursor1.valid() && cursor2.valid() {
            match cursor1.current_key().cmp(cursor2.current_key()) {
                Ordering::Less => {
                    let item = cursor1.current_item();
                    if filter(item.0) {
                        self.writer.write0((&item.0, &item.1)).unwrap();
                    }
                    cursor1.step();
                }
                Ordering::Equal => {
                    cursor1.current_diff().add(cursor2.current_diff(), &mut sum);

                    if !sum.is_zero() && filter(cursor1.current_key()) {
                        self.writer.write0(((cursor1.current_key()), &sum)).unwrap();
                    }
                    cursor1.step();
                    cursor2.step();
                }

                Ordering::Greater => {
                    let item = cursor2.current_item();
                    if filter(item.0) {
                        self.writer.write0((&item.0, &item.1)).unwrap();
                    }
                    cursor2.step();
                }
            }
        }

        while let Some(item) = cursor1.get_current_item() {
            if filter(item.0) {
                self.writer.write0((&item.0, &item.1)).unwrap();
            }
            cursor1.step();
        }
        while let Some(item) = cursor2.get_current_item() {
            if filter(item.0) {
                self.writer.write0((&item.0, &item.1)).unwrap();
            }
            cursor2.step();
        }
    }
}

impl<K, R> TupleBuilder for FileColumnLayerBuilder<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn new(factories: &FileLeafFactories<K, R>) -> Self {
        Self {
            factories: factories.clone(),
            writer: Writer1::new(
                &factories.file_factories,
                &Runtime::storage(),
                Parameters::default(),
            )
            .unwrap(),
        }
    }

    fn with_capacity(factories: &FileLeafFactories<K, R>, _capacity: usize) -> Self {
        Self::new(factories)
    }

    fn reserve_tuples(&mut self, _additional: usize) {}

    fn tuples(&self) -> usize {
        self.writer.n_rows() as usize
    }

    fn push_tuple(&mut self, item: (&mut K, &mut R)) {
        self.writer.write0((item.0, item.1)).unwrap();
    }

    fn push_refs<'a>(&mut self, item: (&K, &R)) {
        self.writer.write0((item.0, item.1)).unwrap();
    }
}

impl<K, R> SizeOf for FileColumnLayerBuilder<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
    }
}
