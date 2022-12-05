use crate::DeCollectionHandle;
use erased_serde::{deserialize, Deserializer as ErasedDeserializer, Error as EError};
use serde::Deserialize;
use std::{
    mem::take,
    sync::{Arc, Mutex, MutexGuard},
};

/// Inner state of `MockDeZSet`.
pub struct MockDeZSetState<T> {
    /// Buffered records that haven't been flushed yet.
    pub buffered: Vec<(T, bool)>,

    /// Records flushed since the last `reset`.
    pub flushed: Vec<(T, bool)>,
}

impl<T> Default for MockDeZSetState<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> MockDeZSetState<T> {
    pub fn new() -> Self {
        Self {
            buffered: Vec::new(),
            flushed: Vec::new(),
        }
    }

    /// Clear internal state.
    pub fn reset(&mut self) {
        self.buffered.clear();
        self.flushed.clear();
    }
}

pub struct MockDeZSet<T>(Arc<Mutex<MockDeZSetState<T>>>);

impl<T> Default for MockDeZSet<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Mock implementation of `DeCollectionHandle`.
impl<T> Clone for MockDeZSet<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> MockDeZSet<T> {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(MockDeZSetState::new())))
    }

    pub fn reset(&self) {
        self.0.lock().unwrap().reset();
    }

    pub fn state(&self) -> MutexGuard<MockDeZSetState<T>> {
        self.0.lock().unwrap()
    }
}

impl<T> DeCollectionHandle for MockDeZSet<T>
where
    T: for<'de> Deserialize<'de> + Send + 'static,
{
    fn insert(&mut self, deserializer: &mut dyn ErasedDeserializer) -> Result<(), EError> {
        let val = deserialize::<T>(deserializer)?;
        self.0.lock().unwrap().buffered.push((val, true));
        Ok(())
    }

    fn delete(&mut self, deserializer: &mut dyn ErasedDeserializer) -> Result<(), EError> {
        let val = deserialize::<T>(deserializer)?;
        self.0.lock().unwrap().buffered.push((val, false));
        Ok(())
    }

    fn reserve(&mut self, _reservation: usize) {}

    fn flush(&mut self) {
        let mut state = self.0.lock().unwrap();

        let mut buffered = take(&mut state.buffered);
        state.flushed.append(&mut buffered);
    }

    fn clear_buffer(&mut self) {
        self.0.lock().unwrap().buffered.clear();
    }

    fn fork(&self) -> Box<dyn DeCollectionHandle> {
        Box::new(self.clone())
    }
}
