use crate::{
    catalog::{DeCollectionStream, RecordFormat},
    static_compile::deinput::{
        CsvDeserializerFromBytes, DeserializerFromBytes, JsonDeserializerFromBytes,
    },
    ControllerError, DeCollectionHandle, DeserializeWithContext, SqlDeserializerConfig,
};
use anyhow::Result as AnyResult;
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
    T: for<'de> DeserializeWithContext<'de, SqlDeserializerConfig> + Send + 'static,
{
    fn configure_deserializer(
        &self,
        record_format: RecordFormat,
    ) -> Result<Box<dyn DeCollectionStream>, ControllerError> {
        match record_format {
            RecordFormat::Csv => Ok(Box::new(
                MockDeZSetStream::<CsvDeserializerFromBytes<_>, T>::new(
                    self.clone(),
                    SqlDeserializerConfig::default(),
                ),
            )),
            RecordFormat::Json(flavor) => Ok(Box::new(MockDeZSetStream::<
                JsonDeserializerFromBytes<SqlDeserializerConfig>,
                T,
            >::new(
                self.clone(),
                SqlDeserializerConfig::from(flavor),
            ))),
        }
    }
}

#[derive(Clone)]
pub struct MockDeZSetStream<De, T> {
    handle: MockDeZSet<T>,
    deserializer: De,
    config: SqlDeserializerConfig,
}

impl<De, T> MockDeZSetStream<De, T>
where
    De: DeserializerFromBytes<SqlDeserializerConfig>,
{
    pub fn new(handle: MockDeZSet<T>, config: SqlDeserializerConfig) -> Self {
        Self {
            handle,
            deserializer: De::create(config.clone()),
            config,
        }
    }
}

impl<De, T> DeCollectionStream for MockDeZSetStream<De, T>
where
    T: for<'de> DeserializeWithContext<'de, SqlDeserializerConfig> + Send + 'static,
    De: DeserializerFromBytes<SqlDeserializerConfig> + Send + 'static,
{
    fn insert(&mut self, data: &[u8]) -> AnyResult<()> {
        let val = DeserializerFromBytes::deserialize::<T>(&mut self.deserializer, data)?;
        self.handle.0.lock().unwrap().buffered.push((val, true));
        Ok(())
    }

    fn delete(&mut self, data: &[u8]) -> AnyResult<()> {
        let val = DeserializerFromBytes::deserialize::<T>(&mut self.deserializer, data)?;
        self.handle.0.lock().unwrap().buffered.push((val, false));
        Ok(())
    }

    fn reserve(&mut self, _reservation: usize) {}

    fn flush(&mut self) {
        let mut state = self.handle.0.lock().unwrap();

        let mut buffered = take(&mut state.buffered);
        state.flushed.append(&mut buffered);
    }

    fn clear_buffer(&mut self) {
        self.handle.0.lock().unwrap().buffered.clear();
    }

    fn fork(&self) -> Box<dyn DeCollectionStream> {
        Box::new(Self::new(self.handle.clone(), self.config.clone()))
    }
}
