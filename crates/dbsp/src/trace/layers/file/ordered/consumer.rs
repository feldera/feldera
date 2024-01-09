use crate::{
    trace::{
        layers::{Cursor, Trie},
        Consumer, ValueConsumer,
    },
    DBData, DBWeight,
};
use ouroboros::self_referencing;

use super::{FileOrderedCursor, FileOrderedLayer, FileOrderedValueCursor};

/// A [`Consumer`] implementation for [`FileOrderedLayer`]s that contain
/// [`ColumnLayer`]s
// TODO: Fuzz testing for correctness and drop safety
#[self_referencing]
pub struct FileOrderedLayerConsumer<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    storage: FileOrderedLayer<K, V, R>,
    #[borrows(storage)]
    #[covariant]
    cursor: FileOrderedCursor<'this, K, V, R>,
}

impl<K, V, R> Consumer<K, V, R, ()> for FileOrderedLayerConsumer<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    type ValueConsumer<'a> = FileOrderedLayerValues<'a, K, V, R>
    where
        Self: 'a;

    fn key_valid(&self) -> bool {
        self.with_cursor(|cursor| cursor.valid())
    }

    fn peek_key(&self) -> &K {
        self.with_cursor(|cursor| cursor.current_key())
    }

    fn next_key(&mut self) -> (K, Self::ValueConsumer<'_>) {
        self.with_mut(|fields| {
            let (key, values) = fields.cursor.take_current_key_and_values().unwrap();
            (
                key,
                FileOrderedLayerValuesBuilder {
                    storage: fields.storage,
                    cursor_builder: |_| values,
                }
                .build(),
            )
        })
    }

    fn seek_key(&mut self, key: &K)
    where
        K: Ord,
    {
        self.with_cursor_mut(|cursor| cursor.seek(key))
    }
}

impl<K, V, R> From<FileOrderedLayer<K, V, R>> for FileOrderedLayerConsumer<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    fn from(layer: FileOrderedLayer<K, V, R>) -> Self {
        FileOrderedLayerConsumerBuilder {
            storage: layer,
            cursor_builder: |storage| storage.cursor(),
        }
        .build()
    }
}

/// A [`ValueConsumer`] impl for the values yielded by
/// [`FileOrderedLayerConsumer`]
#[self_referencing]
pub struct FileOrderedLayerValues<'a, K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    storage: &'a FileOrderedLayer<K, V, R>,
    #[borrows(storage)]
    #[covariant]
    cursor: FileOrderedValueCursor<'this, K, V, R>,
}

impl<'a, K, V, R> ValueConsumer<'a, V, R, ()> for FileOrderedLayerValues<'a, K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    fn value_valid(&self) -> bool {
        self.with_cursor(|cursor| cursor.valid())
    }

    fn next_value(&mut self) -> (V, R, ()) {
        self.with_cursor_mut(|cursor| {
            let (value, diff) = cursor.take_current_item().unwrap();
            (value, diff, ())
        })
    }

    fn remaining_values(&self) -> usize {
        self.with_cursor(|cursor| cursor.remaining_rows() as usize)
    }
}
