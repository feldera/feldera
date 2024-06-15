use crate::catalog::ArrowStream;
use crate::{
    catalog::{DeCollectionStream, RecordFormat},
    format::byte_record_deserializer,
    ControllerError, DeCollectionHandle,
};
use anyhow::{anyhow, bail, Result as AnyResult};
use arrow::array::RecordBatch;
use dbsp::{
    algebra::HasOne, operator::Update, utils::Tup2, DBData, InputHandle, MapHandle, SetHandle,
    ZSetHandle, ZWeight,
};
use pipeline_types::serde_with_context::{DeserializeWithContext, SqlSerdeConfig};
use serde_arrow::Deserializer as ArrowDeserializer;
use std::{collections::VecDeque, marker::PhantomData, ops::Neg};

/// A deserializer that parses byte arrays into a strongly typed representation.
pub trait DeserializerFromBytes<C> {
    /// Create an instance of a deserializer.
    fn create(config: C) -> Self;

    /// Parse an object of type `T` from `data`.
    fn deserialize<T>(&mut self, data: &[u8]) -> AnyResult<T>
    where
        T: for<'de> DeserializeWithContext<'de, C>;
}

/// Deserializer for CSV-encoded data.
pub struct CsvDeserializerFromBytes<C> {
    // CSV deserializer maintains some allocations across invocations,
    // so we keep an instance here.
    reader: csv::Reader<VecDeque<u8>>,
    // Byte record to read CSV records into.
    record: csv::ByteRecord,
    config: C,
}

impl<C> DeserializerFromBytes<C> for CsvDeserializerFromBytes<C> {
    fn create(config: C) -> Self {
        CsvDeserializerFromBytes {
            reader: csv::ReaderBuilder::new()
                .has_headers(false)
                .flexible(true)
                .from_reader(VecDeque::new()),
            record: csv::ByteRecord::new(),
            config,
        }
    }
    fn deserialize<T>(&mut self, data: &[u8]) -> AnyResult<T>
    where
        T: for<'de> DeserializeWithContext<'de, C>,
    {
        // Push new data to reader.
        self.reader.get_mut().extend(data.iter());
        self.reader.read_byte_record(&mut self.record)?;

        T::deserialize_with_context(
            &mut byte_record_deserializer(&self.record, None),
            &self.config,
        )
        .map_err(|e| anyhow!(e.to_string()))
    }
}

// Deserializer for JSON-encoded data.
pub struct JsonDeserializerFromBytes<C> {
    config: C,
}

impl<C> DeserializerFromBytes<C> for JsonDeserializerFromBytes<C> {
    fn create(config: C) -> Self {
        JsonDeserializerFromBytes { config }
    }
    fn deserialize<T>(&mut self, data: &[u8]) -> AnyResult<T>
    where
        T: for<'de> DeserializeWithContext<'de, C>,
    {
        T::deserialize_with_context(
            &mut serde_json::Deserializer::from_slice(data),
            &self.config,
        )
        .map_err(|e| anyhow!(e.to_string()))
    }
}

/// Maximal buffer size reused across buffer flushes.
///
/// Input handles in this module use an internal buffer for input records.
/// We want to reuse the allocation across batches, but we
/// limit the max amount of capacity we reuse so that a very large
/// transaction does not leave a huge unused memory buffer.
const MAX_REUSABLE_CAPACITY: usize = 100_000;

/// An input handle that allows pushing serialized data to an
/// [`InputHandle`].
pub trait DeScalarHandle: Send {
    /// Create a [`DeScalarStream`] object to parse input data encoded
    /// using the format specified in `RecordFormat`.
    fn configure_deserializer(
        &self,
        record_format: RecordFormat,
    ) -> Result<Box<dyn DeScalarStream>, ControllerError>;
}

/// An input handle that deserializes values before pushing them to
/// a stream.
///
/// Handles of this type are produced by the
/// [`DeScalarHandle::configure_deserializer`] method.
///
/// A trait for a type that wraps around an [`InputHandle`] and
/// pushes serialized data to an input stream.  The client of this trait
/// supplies values serialized using the format specified as an argument
/// to [`DeScalarHandle::configure_deserializer`].
///
/// Note: this trait is useful for feeding scalar values to the circuit.
/// Use [`DeCollectionStream`] for relational data.
pub trait DeScalarStream: Send {
    /// Deserialize an input value and push it to the circuit via
    /// [`InputHandle::set_for_worker`].
    ///
    /// Returns an error if deserialization fails, i.e., the serialized
    /// representation is corrupted or does not match the type of the underlying
    /// input stream.
    fn set_for_worker(&mut self, worker: usize, data: &[u8]) -> AnyResult<()>;

    /// Deserialize an input value and push it to the circuit via
    /// [`InputHandle::set_for_all`].
    ///
    /// Returns an error if deserialization fails, i.e., the serialized
    /// representation is corrupted or does not match the type of the underlying
    /// input stream.
    fn set_for_all(&mut self, data: &[u8]) -> AnyResult<()>;

    /// Invoke [`InputHandle::clear_for_all`].
    fn clear_for_all(&mut self);

    /// Create a new handle connected to the same input stream.
    fn fork(&self) -> Box<dyn DeScalarStream>;
}

#[derive(Clone)]
pub struct DeScalarHandleImpl<T, D, F> {
    handle: InputHandle<T>,
    map_func: F,
    phantom: PhantomData<fn(D)>,
}

impl<T, D, F> DeScalarHandleImpl<T, D, F> {
    pub fn new(handle: InputHandle<T>, map_func: F) -> Self {
        Self {
            handle,
            map_func,
            phantom: PhantomData,
        }
    }
}

impl<T, D, F> DeScalarHandle for DeScalarHandleImpl<T, D, F>
where
    T: Default + Send + Clone + 'static,
    D: Default + for<'de> DeserializeWithContext<'de, SqlSerdeConfig> + Send + Clone + 'static,
    F: Fn(D) -> T + Send + Clone + 'static,
{
    fn configure_deserializer(
        &self,
        record_format: RecordFormat,
    ) -> Result<Box<dyn DeScalarStream>, ControllerError> {
        match record_format {
            RecordFormat::Csv => {
                let config = SqlSerdeConfig::default();
                Ok(Box::new(DeScalarStreamImpl::<
                    CsvDeserializerFromBytes<_>,
                    T,
                    D,
                    _,
                    _,
                >::new(
                    self.handle.clone(),
                    self.map_func.clone(),
                    config,
                )))
            }
            RecordFormat::Json(flavor) => {
                let config = SqlSerdeConfig::from(flavor);
                Ok(Box::new(DeScalarStreamImpl::<
                    JsonDeserializerFromBytes<_>,
                    T,
                    D,
                    _,
                    _,
                >::new(
                    self.handle.clone(),
                    self.map_func.clone(),
                    config,
                )))
            }
            RecordFormat::Parquet(_) => {
                todo!()
            }
            #[cfg(feature = "with-avro")]
            RecordFormat::Avro => {
                todo!()
            }
        }
    }
}

struct DeScalarStreamImpl<De, T, D, C, F> {
    handle: InputHandle<T>,
    deserializer: De,
    map_func: F,
    config: C,
    phantom: PhantomData<fn(D)>,
}

impl<De, T, D, C, F> DeScalarStreamImpl<De, T, D, C, F>
where
    De: DeserializerFromBytes<C> + Send,
    C: Clone,
{
    fn new(handle: InputHandle<T>, map_func: F, config: C) -> Self {
        Self {
            handle,
            deserializer: De::create(config.clone()),
            map_func,
            config,
            phantom: PhantomData,
        }
    }
}

impl<De, T, D, C, F> DeScalarStream for DeScalarStreamImpl<De, T, D, C, F>
where
    T: Default + Send + Clone + 'static,
    D: Default + for<'de> DeserializeWithContext<'de, C> + Send + Clone + 'static,
    De: DeserializerFromBytes<C> + Send + 'static,
    C: Clone + Send + 'static,
    F: Fn(D) -> T + Clone + Send + 'static,
{
    fn set_for_worker(&mut self, worker: usize, data: &[u8]) -> AnyResult<()> {
        let val = self.deserializer.deserialize(data)?;
        self.handle.set_for_worker(worker, (self.map_func)(val));
        Ok(())
    }

    fn set_for_all(&mut self, data: &[u8]) -> AnyResult<()> {
        let val = self.deserializer.deserialize(data)?;
        self.handle.set_for_all((self.map_func)(val));
        Ok(())
    }

    fn clear_for_all(&mut self) {
        self.handle.clear_for_all()
    }

    fn fork(&self) -> Box<dyn DeScalarStream> {
        Box::new(Self::new(
            self.handle.clone(),
            self.map_func.clone(),
            self.config.clone(),
        ))
    }
}

pub struct DeZSetHandle<K, D> {
    handle: ZSetHandle<K>,
    phantom: PhantomData<D>,
}

impl<K, D> DeZSetHandle<K, D> {
    pub fn new(handle: ZSetHandle<K>) -> Self {
        Self {
            handle,
            phantom: PhantomData,
        }
    }
}

impl<K, D> DeCollectionHandle for DeZSetHandle<K, D>
where
    K: DBData + From<D>,
    D: for<'de> DeserializeWithContext<'de, SqlSerdeConfig> + Send + 'static,
{
    fn configure_deserializer(
        &self,
        record_format: RecordFormat,
    ) -> Result<Box<dyn DeCollectionStream>, ControllerError> {
        match record_format {
            RecordFormat::Csv => {
                let config = SqlSerdeConfig::default();
                Ok(Box::new(
                    DeZSetStream::<CsvDeserializerFromBytes<_>, K, D, _>::new(
                        self.handle.clone(),
                        config,
                    ),
                ))
            }
            RecordFormat::Json(flavor) => {
                let config = SqlSerdeConfig::from(flavor);
                Ok(Box::new(DeZSetStream::<
                    JsonDeserializerFromBytes<_>,
                    K,
                    D,
                    _,
                >::new(self.handle.clone(), config)))
            }
            RecordFormat::Parquet(_) => {
                todo!()
            }
            #[cfg(feature = "with-avro")]
            RecordFormat::Avro => {
                todo!()
            }
        }
    }

    fn configure_arrow_deserializer(
        &self,
        config: SqlSerdeConfig,
    ) -> Result<Box<dyn ArrowStream>, ControllerError> {
        Ok(Box::new(ArrowZSetStream::new(self.handle.clone(), config)))
    }
}

/// An input handle that wraps a [`MapHandle<K, R>`](`MapHandle`)
/// returned by
/// [`RootCircuit::add_input_zset`](`dbsp::RootCircuit::add_input_zset`).
///
/// The [`insert`](`Self::insert`) method of this handle deserializes value
/// `d` of type `D`, converts it to value `k: K` using the `From` trait and
/// buffers a `(k, +1)` update for the underlying `CollectionHandle`.
///
/// The [`delete`](`Self::delete`) method of this handle buffers a `(k, -1)`
/// update for the underlying `CollectionHandle`.
pub struct DeZSetStream<De, K, D, C> {
    updates: Vec<Tup2<K, ZWeight>>,
    handle: ZSetHandle<K>,
    deserializer: De,
    config: C,
    phantom: PhantomData<D>,
}

impl<De, K, D, C> DeZSetStream<De, K, D, C>
where
    De: DeserializerFromBytes<C>,
    C: Clone,
{
    pub fn new(handle: ZSetHandle<K>, config: C) -> Self {
        Self {
            updates: Vec::new(),
            handle,
            deserializer: De::create(config.clone()),
            config,
            phantom: PhantomData,
        }
    }

    fn clear(&mut self) {
        if self.updates.capacity() > MAX_REUSABLE_CAPACITY {
            self.updates = Vec::new();
        }
    }
}

impl<De, K, D, C> DeCollectionStream for DeZSetStream<De, K, D, C>
where
    De: DeserializerFromBytes<C> + Send + 'static,
    C: Clone + Send + 'static,
    K: DBData + From<D>,
    D: for<'de> DeserializeWithContext<'de, C> + Send + 'static,
{
    fn insert(&mut self, data: &[u8]) -> AnyResult<()> {
        let key = <K as From<D>>::from(self.deserializer.deserialize::<D>(data)?);

        self.updates.push(Tup2(key, ZWeight::one()));
        Ok(())
    }

    fn delete(&mut self, data: &[u8]) -> AnyResult<()> {
        let key = <K as From<D>>::from(self.deserializer.deserialize::<D>(data)?);

        self.updates.push(Tup2(key, ZWeight::one().neg()));
        Ok(())
    }

    fn update(&mut self, _data: &[u8]) -> AnyResult<()> {
        bail!("update operation is not supported on this stream")
    }

    fn reserve(&mut self, reservation: usize) {
        self.updates.reserve(reservation);
    }

    fn flush(&mut self) {
        self.handle.append(&mut self.updates);
        self.clear();
    }

    fn clear_buffer(&mut self) {
        self.updates.clear();
        self.clear();
    }

    fn fork(&self) -> Box<dyn DeCollectionStream> {
        Box::new(Self::new(self.handle.clone(), self.config.clone()))
    }
}

pub struct ArrowZSetStream<K, D, C> {
    handle: ZSetHandle<K>,
    config: C,
    phantom: PhantomData<D>,
}

impl<K, D, C> ArrowZSetStream<K, D, C> {
    pub fn new(handle: ZSetHandle<K>, config: C) -> Self {
        Self {
            handle,
            config,
            phantom: PhantomData,
        }
    }
}

impl<K, D, C> ArrowStream for ArrowZSetStream<K, D, C>
where
    K: DBData + From<D>,
    D: for<'de> DeserializeWithContext<'de, C> + Send + 'static,
    C: Clone + Send + 'static,
{
    fn insert(&mut self, data: &RecordBatch) -> AnyResult<()> {
        let deserializer = ArrowDeserializer::from_record_batch(data)?;
        let records = Vec::<D>::deserialize_with_context(deserializer, &self.config)?;
        self.handle
            .append(&mut records.into_iter().map(|r| Tup2(K::from(r), 1)).collect());

        Ok(())
    }

    fn delete(&mut self, data: &RecordBatch) -> AnyResult<()> {
        let deserializer = ArrowDeserializer::from_record_batch(data)?;
        let records = Vec::<D>::deserialize_with_context(deserializer, &self.config)?;
        self.handle
            .append(&mut records.into_iter().map(|r| Tup2(K::from(r), -1)).collect());

        Ok(())
    }

    fn fork(&self) -> Box<dyn ArrowStream> {
        Box::new(Self::new(self.handle.clone(), self.config.clone()))
    }
}

pub struct DeSetHandle<K, D> {
    handle: SetHandle<K>,
    phantom: PhantomData<D>,
}

impl<K, D> DeSetHandle<K, D> {
    pub fn new(handle: SetHandle<K>) -> Self {
        Self {
            handle,
            phantom: PhantomData,
        }
    }
}

impl<K, D> DeCollectionHandle for DeSetHandle<K, D>
where
    K: DBData + From<D>,
    D: for<'de> DeserializeWithContext<'de, SqlSerdeConfig> + Send + 'static,
{
    fn configure_deserializer(
        &self,
        record_format: RecordFormat,
    ) -> Result<Box<dyn DeCollectionStream>, ControllerError> {
        match record_format {
            RecordFormat::Csv => Ok(Box::new(
                DeSetStream::<CsvDeserializerFromBytes<_>, K, D, _>::new(
                    self.handle.clone(),
                    SqlSerdeConfig::default(),
                ),
            )),
            RecordFormat::Json(flavor) => {
                Ok(Box::new(
                    DeSetStream::<JsonDeserializerFromBytes<_>, K, D, _>::new(
                        self.handle.clone(),
                        SqlSerdeConfig::from(flavor),
                    ),
                ))
            }
            RecordFormat::Parquet(_) => {
                todo!()
            }
            #[cfg(feature = "with-avro")]
            RecordFormat::Avro => {
                todo!()
            }
        }
    }

    fn configure_arrow_deserializer(
        &self,
        config: SqlSerdeConfig,
    ) -> Result<Box<dyn ArrowStream>, ControllerError> {
        Ok(Box::new(ArrowSetStream::new(self.handle.clone(), config)))
    }
}

/// An input handle that wraps a [`SetHandle<V>`](`SetHandle`)
/// returned by
/// [`RootCircuit::add_input_set`](`dbsp::RootCircuit::add_input_set`).
///
/// The [`insert`](`Self::insert`) method of this handle deserializes value
/// `v` type `V` and buffers a `(v, true)` update for the underlying
/// `SetHandle`.
///
/// The [`delete`](`Self::delete`) method of this handle deserializes value
/// `v` type `V` and buffers a `(v, false)` update for the underlying
/// `SetHandle`.
pub struct DeSetStream<De, K, D, C> {
    updates: Vec<Tup2<K, bool>>,
    handle: SetHandle<K>,
    deserializer: De,
    config: C,
    phantom: PhantomData<fn(D)>,
}

impl<De, K, D, C> DeSetStream<De, K, D, C>
where
    De: DeserializerFromBytes<C>,
    C: Clone,
{
    pub fn new(handle: SetHandle<K>, config: C) -> Self {
        Self {
            updates: Vec::new(),
            handle,
            deserializer: De::create(config.clone()),
            config,
            phantom: PhantomData,
        }
    }
}

impl<De, K, D, C> DeCollectionStream for DeSetStream<De, K, D, C>
where
    De: DeserializerFromBytes<C> + Send + 'static,
    C: Clone + Send + 'static,
    K: DBData + From<D>,
    D: for<'de> DeserializeWithContext<'de, C> + Send + 'static,
{
    fn insert(&mut self, data: &[u8]) -> AnyResult<()> {
        let key = <K as From<D>>::from(self.deserializer.deserialize::<D>(data)?);

        self.updates.push(Tup2(key, true));
        Ok(())
    }

    fn delete(&mut self, data: &[u8]) -> AnyResult<()> {
        let key = <K as From<D>>::from(self.deserializer.deserialize::<D>(data)?);

        self.updates.push(Tup2(key, false));
        Ok(())
    }

    fn update(&mut self, _data: &[u8]) -> AnyResult<()> {
        bail!("update operation is not supported on this stream")
    }

    fn reserve(&mut self, reservation: usize) {
        self.updates.reserve(reservation);
    }

    fn flush(&mut self) {
        self.handle.append(&mut self.updates);
        self.updates.shrink_to(MAX_REUSABLE_CAPACITY);
    }

    fn clear_buffer(&mut self) {
        self.updates.clear();
        self.updates.shrink_to(MAX_REUSABLE_CAPACITY);
    }

    fn fork(&self) -> Box<dyn DeCollectionStream> {
        Box::new(Self::new(self.handle.clone(), self.config.clone()))
    }
}

pub struct ArrowSetStream<K, D, C> {
    handle: SetHandle<K>,
    config: C,
    phantom: PhantomData<D>,
}

impl<K, D, C> ArrowSetStream<K, D, C> {
    pub fn new(handle: SetHandle<K>, config: C) -> Self {
        Self {
            handle,
            config,
            phantom: PhantomData,
        }
    }
}

impl<K, D, C> ArrowStream for ArrowSetStream<K, D, C>
where
    K: DBData + From<D>,
    D: for<'de> DeserializeWithContext<'de, C> + Send + 'static,
    C: Clone + Send + 'static,
{
    fn insert(&mut self, data: &RecordBatch) -> AnyResult<()> {
        let deserializer = ArrowDeserializer::from_record_batch(data)?;
        let records = Vec::<D>::deserialize_with_context(deserializer, &self.config)?;
        self.handle.append(
            &mut records
                .into_iter()
                .map(|r| Tup2(K::from(r), true))
                .collect(),
        );

        Ok(())
    }

    fn delete(&mut self, data: &RecordBatch) -> AnyResult<()> {
        let deserializer = ArrowDeserializer::from_record_batch(data)?;
        let records = Vec::<D>::deserialize_with_context(deserializer, &self.config)?;
        self.handle.append(
            &mut records
                .into_iter()
                .map(|r| Tup2(K::from(r), false))
                .collect(),
        );

        Ok(())
    }

    fn fork(&self) -> Box<dyn ArrowStream> {
        Box::new(Self::new(self.handle.clone(), self.config.clone()))
    }
}

pub struct DeMapHandle<K, KD, V, VD, U, UD, VF, UF>
where
    V: DBData,
    U: DBData,
{
    handle: MapHandle<K, V, U>,
    value_key_func: VF,
    update_key_func: UF,
    phantom: PhantomData<fn(KD, VD, UD)>,
}

impl<K, KD, V, VD, U, UD, VF, UF> DeMapHandle<K, KD, V, VD, U, UD, VF, UF>
where
    V: DBData,
    U: DBData,
{
    pub fn new(handle: MapHandle<K, V, U>, value_key_func: VF, update_key_func: UF) -> Self {
        Self {
            handle,
            value_key_func,
            update_key_func,
            phantom: PhantomData,
        }
    }
}

impl<K, KD, V, VD, U, UD, VF, UF> DeCollectionHandle for DeMapHandle<K, KD, V, VD, U, UD, VF, UF>
where
    K: DBData + From<KD>,
    KD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig> + Send + 'static,
    V: DBData + From<VD>,
    VD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig> + Send + 'static,
    U: DBData + From<UD>,
    UD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig> + Send + 'static,
    VF: Fn(&V) -> K + Clone + Send + 'static,
    UF: Fn(&U) -> K + Clone + Send + 'static,
{
    fn configure_deserializer(
        &self,
        record_format: RecordFormat,
    ) -> Result<Box<dyn DeCollectionStream>, ControllerError> {
        match record_format {
            RecordFormat::Csv => Ok(Box::new(DeMapStream::<
                CsvDeserializerFromBytes<_>,
                K,
                KD,
                V,
                VD,
                U,
                UD,
                VF,
                UF,
                _,
            >::new(
                self.handle.clone(),
                self.value_key_func.clone(),
                self.update_key_func.clone(),
                SqlSerdeConfig::default(),
            ))),
            RecordFormat::Json(flavor) => Ok(Box::new(DeMapStream::<
                JsonDeserializerFromBytes<_>,
                K,
                KD,
                V,
                VD,
                U,
                UD,
                VF,
                UF,
                _,
            >::new(
                self.handle.clone(),
                self.value_key_func.clone(),
                self.update_key_func.clone(),
                SqlSerdeConfig::from(flavor),
            ))),
            RecordFormat::Parquet(_) => {
                todo!()
            }
            #[cfg(feature = "with-avro")]
            RecordFormat::Avro => {
                todo!()
            }
        }
    }

    fn configure_arrow_deserializer(
        &self,
        config: SqlSerdeConfig,
    ) -> Result<Box<dyn ArrowStream>, ControllerError> {
        Ok(Box::new(ArrowMapStream::new(
            self.handle.clone(),
            self.value_key_func.clone(),
            config,
        )))
    }
}

/// An input handle that wraps a [`MapHandle<K, V>`](`MapHandle`)
/// returned by
/// [`RootCircuit::add_input_map`](`dbsp::RootCircuit::add_input_map`).
///
/// The [`insert`](`Self::insert`) method of this handle deserializes value
/// `v` type `V` and buffers a `(key_func(v), Some(v))` update for the
/// underlying `MapHandle`, where `key_func: F` extracts key of type `K`
/// from value of type `V`.
///
/// The [`delete`](`Self::delete`) method of this handle deserializes value
/// `k` type `K` and buffers a `(k, None)` update for the underlying
/// `MapHandle`.
pub struct DeMapStream<De, K, KD, V, VD, U, UD, VF, UF, C>
where
    V: DBData,
    U: DBData,
{
    updates: Vec<Tup2<K, Update<V, U>>>,
    value_key_func: VF,
    update_key_func: UF,
    handle: MapHandle<K, V, U>,
    config: C,
    deserializer: De,
    phantom: PhantomData<fn(KD, VD, UD)>,
}

impl<De, K, KD, V, VD, U, UD, VF, UF, C> DeMapStream<De, K, KD, V, VD, U, UD, VF, UF, C>
where
    V: DBData,
    U: DBData,
    De: DeserializerFromBytes<C>,
    C: Clone,
{
    pub fn new(
        handle: MapHandle<K, V, U>,
        value_key_func: VF,
        update_key_func: UF,
        config: C,
    ) -> Self {
        Self {
            updates: Vec::new(),
            value_key_func,
            update_key_func,
            handle,
            deserializer: De::create(config.clone()),
            config,
            phantom: PhantomData,
        }
    }
}

impl<De, K, KD, V, VD, U, UD, VF, UF, C> DeCollectionStream
    for DeMapStream<De, K, KD, V, VD, U, UD, VF, UF, C>
where
    De: DeserializerFromBytes<C> + Send + 'static,
    C: Clone + Send + 'static,
    K: DBData + From<KD>,
    KD: for<'de> DeserializeWithContext<'de, C> + Send + 'static,
    V: DBData + From<VD>,
    VD: for<'de> DeserializeWithContext<'de, C> + Send + 'static,
    U: DBData + From<UD>,
    UD: for<'de> DeserializeWithContext<'de, C> + Send + 'static,
    VF: Fn(&V) -> K + Clone + Send + 'static,
    UF: Fn(&U) -> K + Clone + Send + 'static,
{
    fn insert(&mut self, data: &[u8]) -> AnyResult<()> {
        let val = V::from(self.deserializer.deserialize::<VD>(data)?);
        let key = (self.value_key_func)(&val);

        self.updates.push(Tup2(key, Update::Insert(val)));
        Ok(())
    }

    fn delete(&mut self, data: &[u8]) -> AnyResult<()> {
        let key = K::from(self.deserializer.deserialize::<KD>(data)?);

        self.updates.push(Tup2(key, Update::Delete));
        Ok(())
    }

    fn update(&mut self, data: &[u8]) -> AnyResult<()> {
        let upd = U::from(self.deserializer.deserialize::<UD>(data)?);
        let key = (self.update_key_func)(&upd);

        self.updates.push(Tup2(key, Update::Update(upd)));
        Ok(())
    }

    fn reserve(&mut self, reservation: usize) {
        self.updates.reserve(reservation);
    }

    fn flush(&mut self) {
        self.handle.append(&mut self.updates);
        self.updates.shrink_to(MAX_REUSABLE_CAPACITY);
    }

    fn clear_buffer(&mut self) {
        self.updates.clear();
        self.updates.shrink_to(MAX_REUSABLE_CAPACITY);
    }

    fn fork(&self) -> Box<dyn DeCollectionStream> {
        Box::new(Self::new(
            self.handle.clone(),
            self.value_key_func.clone(),
            self.update_key_func.clone(),
            self.config.clone(),
        ))
    }
}

pub struct ArrowMapStream<K, KD, V, VD, U, VF, C>
where
    V: DBData,
    U: DBData,
{
    value_key_func: VF,
    handle: MapHandle<K, V, U>,
    config: C,
    phantom: PhantomData<fn(KD, VD)>,
}

impl<K, KD, V, VD, U, VF, C> ArrowMapStream<K, KD, V, VD, U, VF, C>
where
    V: DBData,
    U: DBData,
    C: Clone,
{
    pub fn new(handle: MapHandle<K, V, U>, value_key_func: VF, config: C) -> Self {
        Self {
            value_key_func,
            handle,
            config,
            phantom: PhantomData,
        }
    }
}

impl<K, KD, V, VD, U, VF, C> ArrowStream for ArrowMapStream<K, KD, V, VD, U, VF, C>
where
    C: Clone + Send + 'static,
    K: DBData + From<KD>,
    KD: for<'de> DeserializeWithContext<'de, C> + Send + 'static,
    V: DBData + From<VD>,
    VD: for<'de> DeserializeWithContext<'de, C> + Send + 'static,
    U: DBData,
    VF: Fn(&V) -> K + Clone + Send + 'static,
{
    fn insert(&mut self, data: &RecordBatch) -> AnyResult<()> {
        let deserializer = ArrowDeserializer::from_record_batch(data)?;
        let records = Vec::<VD>::deserialize_with_context(deserializer, &self.config)?;
        self.handle.append(
            &mut records
                .into_iter()
                .map(|r| {
                    let v = V::from(r);
                    Tup2((self.value_key_func)(&v), Update::Insert(v))
                })
                .collect(),
        );

        Ok(())
    }

    fn delete(&mut self, data: &RecordBatch) -> AnyResult<()> {
        let deserializer = ArrowDeserializer::from_record_batch(data)?;
        let records = Vec::<VD>::deserialize_with_context(deserializer, &self.config)?;
        self.handle.append(
            &mut records
                .into_iter()
                .map(|r| {
                    let v = V::from(r);
                    Tup2((self.value_key_func)(&v), Update::Delete)
                })
                .collect(),
        );

        Ok(())
    }

    fn fork(&self) -> Box<dyn ArrowStream> {
        Box::new(Self::new(
            self.handle.clone(),
            self.value_key_func.clone(),
            self.config.clone(),
        ))
    }
}

#[cfg(test)]
mod test {
    use crate::{
        static_compile::{
            deinput::RecordFormat, DeMapHandle, DeScalarHandle, DeScalarHandleImpl, DeSetHandle,
            DeZSetHandle,
        },
        DeCollectionHandle,
    };
    use csv::WriterBuilder as CsvWriterBuilder;
    use csv_core::{ReadRecordResult, Reader as CsvReader};
    use dbsp::{
        algebra::F32, utils::Tup2, DBSPHandle, OrdIndexedZSet, OrdZSet, OutputHandle, Runtime,
    };

    use pipeline_types::{deserialize_without_context, format::json::JsonFlavor};
    use serde_json::to_string as to_json_string;
    use size_of::SizeOf;
    use std::hash::Hash;

    const NUM_WORKERS: usize = 4;

    #[derive(
        Clone,
        Debug,
        Default,
        Hash,
        PartialEq,
        Eq,
        PartialOrd,
        Ord,
        serde::Deserialize,
        serde::Serialize,
        SizeOf,
        rkyv::Archive,
        rkyv::Serialize,
        rkyv::Deserialize,
    )]
    #[archive_attr(derive(Clone, Ord, Eq, PartialEq, PartialOrd))]
    #[archive(compare(PartialEq, PartialOrd))]
    struct TestStruct {
        id: i64,
        s: String,
        b: bool,
        o: Option<F32>,
    }

    deserialize_without_context!(TestStruct);

    type InputHandles = (
        Box<dyn DeCollectionHandle>,
        Box<dyn DeCollectionHandle>,
        Box<dyn DeCollectionHandle>,
    );
    type OutputHandles = (
        OutputHandle<OrdZSet<TestStruct>>,
        OutputHandle<OrdZSet<TestStruct>>,
        OutputHandle<OrdIndexedZSet<i64, TestStruct>>,
    );

    // Test circuit for DeScalarHandle.
    fn descalar_test_circuit(
        workers: usize,
    ) -> (
        DBSPHandle,
        Box<dyn DeScalarHandle>,
        OutputHandle<TestStruct>,
    ) {
        let (dbsp, (input_handle, output_handle)) = Runtime::init_circuit(workers, |circuit| {
            let (input, input_handle) = circuit.add_input_stream::<TestStruct>();
            let output_handle = input.output();

            Ok((input_handle, output_handle))
        })
        .unwrap();

        let input_handle = DeScalarHandleImpl::new(input_handle, |x| x);

        (dbsp, Box::new(input_handle), output_handle)
    }

    #[test]
    fn test_scalar() {
        let (mut dbsp, input_handle, output_handle) = descalar_test_circuit(NUM_WORKERS);

        let inputs = vec![
            TestStruct {
                id: 1,
                s: "foo".to_string(),
                b: true,
                o: Some(F32::from(0.1)),
            },
            TestStruct {
                id: 2,
                s: "bar".to_string(),
                b: false,
                o: None,
            },
            TestStruct {
                id: 3,
                s: "".to_string(),
                b: false,
                o: None,
            },
        ];

        let mut input_stream = input_handle
            .configure_deserializer(RecordFormat::Json(JsonFlavor::Default))
            .unwrap();

        for input in inputs.iter() {
            let input_json = to_json_string(input).unwrap();
            input_stream.set_for_all(input_json.as_bytes()).unwrap();
            dbsp.step().unwrap();

            let outputs = output_handle.take_from_all();
            assert!(outputs.iter().all(|x| x == input));
        }

        let mut input_stream_clone = input_stream.fork();

        for (w, input) in inputs.iter().enumerate() {
            let input_json = to_json_string(input).unwrap();

            input_stream_clone
                .set_for_worker(w % NUM_WORKERS, input_json.as_bytes())
                .unwrap();
            dbsp.step().unwrap();

            let output = output_handle.take_from_worker(w % NUM_WORKERS).unwrap();
            assert_eq!(&output, input);
        }

        dbsp.kill().unwrap();
    }

    // Test circuit for DeCollectionHandle handles.
    fn decollection_test_circuit(workers: usize) -> (DBSPHandle, InputHandles, OutputHandles) {
        let (dbsp, ((zset_input, zset_output), (set_input, set_output), (map_input, map_output))) =
            Runtime::init_circuit(workers, |circuit| {
                let (zset, zset_handle) = circuit.add_input_zset::<TestStruct>();
                let (set, set_handle) = circuit.add_input_set::<TestStruct>();
                let (map, map_handle) =
                    circuit.add_input_map::<i64, TestStruct, TestStruct, _>(|v, u| *v = u.clone());

                let zset_output = zset.output();
                let set_output = set.output();
                let map_output = map.output();

                Ok((
                    (zset_handle, zset_output),
                    (set_handle, set_output),
                    (map_handle, map_output),
                ))
            })
            .unwrap();

        let de_zset = DeZSetHandle::new(zset_input);
        let de_set = DeSetHandle::new(set_input);
        let de_map: DeMapHandle<i64, i64, _, _, _, _, _, _> = DeMapHandle::new(
            map_input,
            |test_struct: &TestStruct| test_struct.id,
            |test_struct: &TestStruct| test_struct.id,
        );

        (
            dbsp,
            (Box::new(de_zset), Box::new(de_set), Box::new(de_map)),
            (zset_output, set_output, map_output),
        )
    }

    // Feed `inputs` in CSV format.
    fn insert_csv(
        dbsp: &mut DBSPHandle,
        input_handles: &InputHandles,
        output_handles: &OutputHandles,
        inputs: &[TestStruct],
    ) {
        let mut zset_stream = input_handles
            .0
            .configure_deserializer(RecordFormat::Csv)
            .unwrap();
        let mut set_stream = input_handles
            .1
            .configure_deserializer(RecordFormat::Csv)
            .unwrap();
        let mut map_stream = input_handles
            .2
            .configure_deserializer(RecordFormat::Csv)
            .unwrap();

        let zset_output = &output_handles.0;
        let set_output = &output_handles.1;
        let map_output = &output_handles.2;

        let zset = OrdZSet::from_keys(
            (),
            inputs
                .iter()
                .map(|v| Tup2(v.clone(), 1))
                .collect::<Vec<_>>(),
        );
        let map = <OrdIndexedZSet<i64, TestStruct>>::from_tuples(
            (),
            inputs
                .iter()
                .map(|v| Tup2(Tup2(v.id, v.clone()), 1))
                .collect::<Vec<_>>(),
        );

        zset_stream.reserve(inputs.len());
        set_stream.reserve(1);
        map_stream.reserve(0);

        // Serialize `inputs` as CSV.
        let mut csv_bytes: Vec<u8> = Vec::new();
        let mut csv_writer = CsvWriterBuilder::new()
            .has_headers(false)
            .from_writer(&mut csv_bytes);
        for input in inputs.iter() {
            csv_writer.serialize(input).unwrap();
        }
        let _ = csv_writer.into_inner().unwrap();
        let csv_string = String::from_utf8(csv_bytes).unwrap();

        // println!("csv:\n{}", csv_string);

        // CSV reader iterates over CSV records.
        let mut csv_reader = CsvReader::new();
        let mut data = csv_string.as_bytes();
        let mut output = vec![0; data.len()];
        let mut ends = [0usize; 10];

        // Make sure that providing a record with incorrect number of columns doesn't
        // prevent subsequent records from parsing correctly (this works thanks to
        // `CsvReaderBuilder::flexible()`).
        zset_stream.insert(b"1,x,x,x,x,x,x,x,x\n").unwrap_err();
        loop {
            let (result, bytes_read, _, _) = csv_reader.read_record(data, &mut output, &mut ends);
            match result {
                ReadRecordResult::End => break,
                ReadRecordResult::Record => {
                    zset_stream.insert(&data[0..bytes_read]).unwrap();
                    set_stream.insert(&data[0..bytes_read]).unwrap();
                    map_stream.insert(&data[0..bytes_read]).unwrap();
                    data = &data[bytes_read..];
                }
                result => panic!("Unexpected result parsing CSV: {result:?}"),
            }
        }

        zset_stream.flush();
        set_stream.flush();
        map_stream.flush();

        dbsp.step().unwrap();

        assert_eq!(zset_output.consolidate(), zset);
        assert_eq!(set_output.consolidate(), zset);
        assert_eq!(map_output.consolidate(), map);
    }

    // Delete `inputs` in JSON format.
    fn delete_json(
        dbsp: &mut DBSPHandle,
        input_handles: &InputHandles,
        output_handles: &OutputHandles,
        inputs: &[TestStruct],
    ) {
        let mut zset_input = input_handles
            .0
            .configure_deserializer(RecordFormat::Json(JsonFlavor::Default))
            .unwrap();
        let mut set_input = input_handles
            .1
            .configure_deserializer(RecordFormat::Json(JsonFlavor::Default))
            .unwrap();
        let mut map_input = input_handles
            .2
            .configure_deserializer(RecordFormat::Json(JsonFlavor::Default))
            .unwrap();

        let zset_output = &output_handles.0;
        let set_output = &output_handles.1;
        let map_output = &output_handles.2;

        let zset = OrdZSet::from_keys(
            (),
            inputs
                .iter()
                .map(|v| Tup2(v.clone(), -1))
                .collect::<Vec<_>>(),
        );
        let map = <OrdIndexedZSet<i64, TestStruct>>::from_tuples(
            (),
            inputs
                .iter()
                .map(|v| Tup2(Tup2(v.id, v.clone()), -1))
                .collect::<Vec<_>>(),
        );

        for input in inputs.iter() {
            let id = input.id;
            let input = to_json_string(input).unwrap();

            zset_input.delete(input.as_bytes()).unwrap();
            zset_input.flush();

            set_input.delete(input.as_bytes()).unwrap();
            set_input.flush();

            let input_id = to_json_string(&id).unwrap();
            map_input.delete(input_id.as_bytes()).unwrap();
            map_input.flush();
        }

        dbsp.step().unwrap();

        assert_eq!(zset_output.consolidate(), zset);
        assert_eq!(set_output.consolidate(), zset);
        assert_eq!(map_output.consolidate(), map);
    }

    #[test]
    fn test_collection() {
        let (mut dbsp, input_handles, output_handles) = decollection_test_circuit(NUM_WORKERS);

        let inputs = vec![
            TestStruct {
                id: 1,
                s: "foo".to_string(),
                b: true,
                o: Some(F32::from(0.1)),
            },
            TestStruct {
                id: 2,
                s: "bar".to_string(),
                b: false,
                o: None,
            },
            TestStruct {
                id: 3,
                s: "".to_string(),
                b: false,
                o: None,
            },
        ];

        insert_csv(&mut dbsp, &input_handles, &output_handles, &inputs);
        delete_json(&mut dbsp, &input_handles, &output_handles, &inputs);

        dbsp.kill().unwrap();
    }
}
