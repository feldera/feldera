use dbsp::{
    trace::{Batch, BatchReader, Cursor},
    OutputHandle,
};
use erased_serde::Serialize as ErasedSerialize;
use serde::Serialize;
use std::sync::Arc;

/// A type-erased batch whose contents can be serialized.
///
/// This is a wrapper around the DBSP `Batch` trait that returns a cursor that
/// yields `erased_serde::Serialize` trait objects that can be used to serialize
/// the contents of the batch without knowing its key and value types.
pub trait SerBatch: Send {
    /// Number of keys in the batch.
    fn key_count(&self) -> usize;

    /// Number of tuples in the batch.
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Cursor over the batch.
    fn cursor<'a>(&'a self) -> Box<dyn SerCursor + 'a>;

    // fn fork(&self) -> Box<dyn SerBatch>;
}

/// Cursor that allows serializing the contents of a type-erased batch.
///
/// This is a wrapper around the DBSP `Cursor` trait that yields keys and values
/// of the underlying batch as `erased_serde::Serialize` trait objects.
pub trait SerCursor {
    /// Indicates if the current key is valid.
    ///
    /// A value of `false` indicates that the cursor has exhausted all keys.
    fn key_valid(&self) -> bool;

    /// Indicates if the current value is valid.
    ///
    /// A value of `false` indicates that the cursor has exhausted all values
    /// for this key.
    fn val_valid(&self) -> bool;

    /// A reference to the current key. Panics if invalid.
    fn key(&self) -> &dyn ErasedSerialize;

    /// A reference to the current value. Panics if invalid.
    fn val(&self) -> &dyn ErasedSerialize;

    /// Returns a reference to the current key, if valid.
    fn get_key(&self) -> Option<&dyn ErasedSerialize> {
        if self.key_valid() {
            Some(self.key())
        } else {
            None
        }
    }

    /// Returns a reference to the current value, if valid.
    fn get_val(&self) -> Option<&dyn ErasedSerialize> {
        if self.val_valid() {
            Some(self.val())
        } else {
            None
        }
    }

    /// Returns the weight associated with the current key/value pair.
    fn weight(&mut self) -> i64;

    /// Advances the cursor to the next key.
    fn step_key(&mut self);

    /// Advances the cursor to the next value.
    fn step_val(&mut self);

    /// Rewinds the cursor to the first key.
    fn rewind_keys(&mut self);

    /// Rewinds the cursor to the first value for current key.
    fn rewind_vals(&mut self);
}

/// [`SerBatch`] implementation that wraps a `BatchReader`.
pub struct SerBatchImpl<B>
where
    B: BatchReader,
{
    /// `Arc` is necessary for this type to satisfy `Send`.
    batch: Arc<B>,
}

impl<B> SerBatchImpl<B>
where
    B: BatchReader,
{
    pub fn new(batch: B) -> Self {
        Self {
            batch: Arc::new(batch),
        }
    }
}

impl<B> SerBatch for SerBatchImpl<B>
where
    B: BatchReader<Time = ()> + Clone + Send + Sync,
    B::Key: Serialize,
    B::Val: Serialize,
    B::R: Into<i64>,
{
    fn key_count(&self) -> usize {
        self.batch.key_count()
    }

    fn len(&self) -> usize {
        self.batch.len()
    }

    fn cursor<'a>(&'a self) -> Box<dyn SerCursor + 'a> {
        Box::new(SerBatchCursor::new(&*self.batch))
    }

    /*fn fork(&self) -> Box<dyn SerBatch> {
        Box::new(Self {
            batch: self.batch.clone(),
        })
    }*/
}

/// [`SerCursor`] implementation that wraps a [`Cursor`].
pub struct SerBatchCursor<'a, B>
where
    B: BatchReader,
{
    cursor: B::Cursor<'a>,
}

impl<'a, B> SerBatchCursor<'a, B>
where
    B: BatchReader,
{
    pub fn new(batch: &'a B) -> Self {
        let cursor = batch.cursor();

        Self { cursor }
    }
}

impl<'a, B> SerCursor for SerBatchCursor<'a, B>
where
    B: BatchReader<Time = ()> + Clone,
    B::Key: Serialize,
    B::Val: Serialize,
    B::R: Into<i64>,
{
    fn key_valid(&self) -> bool {
        self.cursor.key_valid()
    }

    fn val_valid(&self) -> bool {
        self.cursor.val_valid()
    }

    fn key(&self) -> &dyn ErasedSerialize {
        self.cursor.key()
    }

    fn val(&self) -> &dyn ErasedSerialize {
        self.cursor.val()
    }

    fn weight(&mut self) -> i64 {
        self.cursor.weight().into()
    }

    fn step_key(&mut self) {
        self.cursor.step_key();
    }

    /// Advances the cursor to the next value.
    fn step_val(&mut self) {
        self.cursor.step_val();
    }

    /// Rewinds the cursor to the first key.
    fn rewind_keys(&mut self) {
        self.cursor.rewind_keys();
    }

    /// Rewinds the cursor to the first value for current key.
    fn rewind_vals(&mut self) {
        self.cursor.rewind_vals();
    }
}

/// A handle to an output stream of a circuit that yields type-erased
/// output batches.
///
/// A trait for a type that wraps around an [`OutputHandle<Batch>`] and
/// yields output batches produced by the circuit as [`SerBatch`]s.
pub trait SerOutputBatchHandle: Send + Sync {
    /// Like [`OutputHandle::take_from_worker`], but returns output batch as a
    /// [`SerBatch`] trait object.
    fn take_from_worker(&self, worker: usize) -> Option<Box<dyn SerBatch>>;

    /// Like [`OutputHandle::take_from_all`], but returns output batches as
    /// [`SerBatch`] trait objects.
    fn take_from_all(&self) -> Vec<Box<dyn SerBatch>>;

    /// Like [`OutputHandle::consolidate`], but returns the output batch as a
    /// [`SerBatch`] trait object.
    fn consolidate(&self) -> Box<dyn SerBatch>;

    /// Returns an alias to `self`.
    fn fork(&self) -> Box<dyn SerOutputBatchHandle>;
}

impl<B> SerOutputBatchHandle for OutputHandle<B>
where
    B: Batch<Time = ()> + Send + Sync,
    B::Key: Serialize,
    B::Val: Serialize,
    B::R: Into<i64>,
{
    fn take_from_worker(&self, worker: usize) -> Option<Box<dyn SerBatch>> {
        self.take_from_worker(worker)
            .map(|batch| Box::new(SerBatchImpl::new(batch)) as Box<dyn SerBatch>)
    }

    fn take_from_all(&self) -> Vec<Box<dyn SerBatch>> {
        self.take_from_all()
            .into_iter()
            .map(|batch| Box::new(SerBatchImpl::new(batch)) as Box<dyn SerBatch>)
            .collect()
    }

    fn consolidate(&self) -> Box<dyn SerBatch> {
        let batch = self.consolidate();
        Box::new(SerBatchImpl::new(batch))
    }

    fn fork(&self) -> Box<dyn SerOutputBatchHandle> {
        Box::new(self.clone())
    }
}
