use dbsp::{
    trace::{Batch, BatchReader, Cursor},
    OutputHandle,
};
use erased_serde::Serialize as ErasedSerialize;
use serde::Serialize;
use std::{marker::PhantomData, sync::Arc};

/// A type-erased batch whose contents can be serialized.
///
/// This is a wrapper around the DBSP `Batch` trait that returns a cursor that
/// yields `erased_serde::Serialize` trait objects that can be used to serialize
/// the contents of the batch without knowing its key and value types.
pub trait SerBatch: Send + Sync {
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
pub struct SerBatchImpl<B, KD, VD>
where
    B: BatchReader,
{
    /// `Arc` is necessary for this type to satisfy `Send`.
    batch: Arc<B>,
    phantom: PhantomData<fn() -> (KD, VD)>,
}

impl<B, KD, VD> SerBatchImpl<B, KD, VD>
where
    B: BatchReader,
{
    pub fn new(batch: B) -> Self {
        Self {
            batch: Arc::new(batch),
            phantom: PhantomData,
        }
    }
}

impl<B, KD, VD> SerBatch for SerBatchImpl<B, KD, VD>
where
    B: BatchReader<Time = ()> + Clone + Send + Sync,
    B::R: Into<i64>,
    KD: From<B::Key> + Serialize,
    VD: From<B::Val> + Serialize,
{
    fn key_count(&self) -> usize {
        self.batch.key_count()
    }

    fn len(&self) -> usize {
        self.batch.len()
    }

    fn cursor<'a>(&'a self) -> Box<dyn SerCursor + 'a> {
        Box::new(<SerBatchCursor<'a, B, KD, VD>>::new(&*self.batch))
    }

    /*fn fork(&self) -> Box<dyn SerBatch> {
        Box::new(Self {
            batch: self.batch.clone(),
        })
    }*/
}

/// [`SerCursor`] implementation that wraps a [`Cursor`].
pub struct SerBatchCursor<'a, B, KD, VD>
where
    B: BatchReader,
{
    cursor: B::Cursor<'a>,
    phantom: PhantomData<fn() -> (KD, VD)>,
    key: Option<KD>,
    val: Option<VD>,
}

impl<'a, B, KD, VD> SerBatchCursor<'a, B, KD, VD>
where
    B: BatchReader<Time = ()> + Clone,
    B::R: Into<i64>,
    KD: From<B::Key> + Serialize,
    VD: From<B::Val> + Serialize,
{
    pub fn new(batch: &'a B) -> Self {
        let cursor = batch.cursor();

        let mut result = Self {
            cursor,
            key: None,
            val: None,
            phantom: PhantomData,
        };
        result.update_key();
        result.update_val();
        result
    }

    fn update_key(&mut self) {
        if self.key_valid() {
            self.key = Some(KD::from(self.cursor.key().clone()));
        } else {
            self.key = None;
        }
    }

    fn update_val(&mut self) {
        if self.val_valid() {
            self.val = Some(VD::from(self.cursor.val().clone()));
        } else {
            self.val = None;
        }
    }
}

impl<'a, B, KD, VD> SerCursor for SerBatchCursor<'a, B, KD, VD>
where
    B: BatchReader<Time = ()> + Clone,
    B::R: Into<i64>,
    KD: From<B::Key> + Serialize,
    VD: From<B::Val> + Serialize,
{
    fn key_valid(&self) -> bool {
        self.cursor.key_valid()
    }

    fn val_valid(&self) -> bool {
        self.cursor.val_valid()
    }

    fn key(&self) -> &dyn ErasedSerialize {
        self.key.as_ref().unwrap()
    }

    fn val(&self) -> &dyn ErasedSerialize {
        self.val.as_ref().unwrap()
    }

    fn weight(&mut self) -> i64 {
        self.cursor.weight().into()
    }

    fn step_key(&mut self) {
        self.cursor.step_key();
        self.update_key();
        self.update_val();
    }

    /// Advances the cursor to the next value.
    fn step_val(&mut self) {
        self.cursor.step_val();
        self.update_val();
    }

    /// Rewinds the cursor to the first key.
    fn rewind_keys(&mut self) {
        self.cursor.rewind_keys();
        self.update_key();
        self.update_val();
    }

    /// Rewinds the cursor to the first value for current key.
    fn rewind_vals(&mut self) {
        self.cursor.rewind_vals();
        self.update_val();
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
    fn take_from_all(&self) -> Vec<Arc<dyn SerBatch>>;

    /// Like [`OutputHandle::consolidate`], but returns the output batch as a
    /// [`SerBatch`] trait object.
    fn consolidate(&self) -> Box<dyn SerBatch>;

    /// Returns an alias to `self`.
    fn fork(&self) -> Box<dyn SerOutputBatchHandle>;
}

pub struct SerOutputBatchHandleImpl<B, KD, VD> {
    handle: OutputHandle<B>,
    phantom: PhantomData<fn() -> (KD, VD)>,
}

impl<B: Clone, KD, VD> Clone for SerOutputBatchHandleImpl<B, KD, VD> {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            phantom: PhantomData,
        }
    }
}

impl<B, KD, VD> SerOutputBatchHandleImpl<B, KD, VD> {
    pub fn new(handle: OutputHandle<B>) -> Self {
        Self {
            handle,
            phantom: PhantomData,
        }
    }
}

impl<B, KD, VD> SerOutputBatchHandle for SerOutputBatchHandleImpl<B, KD, VD>
where
    B: Batch<Time = ()> + Send + Sync + Clone,
    B::R: Into<i64>,
    KD: From<B::Key> + Serialize + 'static,
    VD: From<B::Val> + Serialize + 'static,
{
    fn take_from_worker(&self, worker: usize) -> Option<Box<dyn SerBatch>> {
        self.handle
            .take_from_worker(worker)
            .map(|batch| Box::new(<SerBatchImpl<B, KD, VD>>::new(batch)) as Box<dyn SerBatch>)
    }

    fn take_from_all(&self) -> Vec<Arc<dyn SerBatch>> {
        self.handle
            .take_from_all()
            .into_iter()
            .map(|batch| Arc::new(<SerBatchImpl<B, KD, VD>>::new(batch)) as Arc<dyn SerBatch>)
            .collect()
    }

    fn consolidate(&self) -> Box<dyn SerBatch> {
        let batch = self.handle.consolidate();
        Box::new(<SerBatchImpl<B, KD, VD>>::new(batch))
    }

    fn fork(&self) -> Box<dyn SerOutputBatchHandle> {
        Box::new(self.clone())
    }
}
