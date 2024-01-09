use std::fmt::{Debug, Formatter, Result as FmtResult};

use crate::{
    trace::{layers::Trie, Consumer, ValueConsumer},
    DBData, DBWeight,
};
use ouroboros::self_referencing;

// Some kind of bug in ouroboros means that the `pub` here is required,
// otherwise code below fails to compile.
pub use crate::trace::layers::Cursor;

use super::{FileColumnLayer, FileColumnLayerCursor};

#[self_referencing]
pub struct FileColumnLayerConsumer<K, R>
where
    K: DBData,
    R: DBWeight,
{
    storage: FileColumnLayer<K, R>,
    #[borrows(storage)]
    #[covariant]
    cursor: FileColumnLayerCursor<'this, K, R>,
}

impl<K, R> Debug for FileColumnLayerConsumer<K, R>
where
    K: DBData,
    R: DBWeight,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "FileColumnLayerConsumer")
    }
}

impl<K, R> Consumer<K, (), R, ()> for FileColumnLayerConsumer<K, R>
where
    K: DBData,
    R: DBWeight,
{
    type ValueConsumer<'a> = FileColumnLayerValues<R>
    where
        Self: 'a;

    fn key_valid(&self) -> bool {
        self.with_cursor(|cursor| cursor.valid())
    }

    fn peek_key(&self) -> &K {
        self.with_cursor(|cursor| cursor.current_key())
    }

    fn next_key(&mut self) -> (K, Self::ValueConsumer<'_>) {
        let (key, diff) = self
            .with_cursor_mut(|cursor| cursor.take_current_item())
            .unwrap();
        (key, FileColumnLayerValues::new(diff))
    }

    fn seek_key(&mut self, key: &K)
    where
        K: Ord,
    {
        self.with_cursor_mut(|cursor| cursor.seek(key));
    }
}

impl<K, R> From<FileColumnLayer<K, R>> for FileColumnLayerConsumer<K, R>
where
    K: DBData,
    R: DBWeight,
{
    #[inline]
    fn from(leaf: FileColumnLayer<K, R>) -> Self {
        FileColumnLayerConsumerBuilder {
            storage: leaf,
            cursor_builder: |storage: &FileColumnLayer<K, R>| storage.cursor(),
        }
        .build()
    }
}

#[derive(Debug)]
pub struct FileColumnLayerValues<R> {
    value: Option<R>,
}

impl<R> FileColumnLayerValues<R> {
    #[inline]
    fn new(value: R) -> Self {
        Self { value: Some(value) }
    }
}

impl<'a, R> ValueConsumer<'a, (), R, ()> for FileColumnLayerValues<R>
where
    R: DBWeight,
{
    fn value_valid(&self) -> bool {
        self.value.is_some()
    }

    fn next_value(&mut self) -> ((), R, ()) {
        ((), self.value.take().unwrap(), ())
    }

    fn remaining_values(&self) -> usize {
        self.value.is_some() as usize
    }
}
