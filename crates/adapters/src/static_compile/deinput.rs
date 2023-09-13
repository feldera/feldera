use crate::{
    catalog::{DeCollectionStream, RecordFormat},
    format::byte_record_deserializer,
    ControllerError, DeCollectionHandle,
};
use anyhow::{anyhow, Result as AnyResult};
use dbsp::{algebra::ZRingValue, CollectionHandle, DBData, DBWeight, InputHandle, UpsertHandle};
use erased_serde::{deserialize, Deserializer as ErasedDeserializer, Error as EError};
use serde::Deserialize;
use std::{collections::VecDeque, marker::PhantomData};

/// A deserializer that parses byte arrays into a strongly typed representation.
pub trait DeserializerFromBytes {
    /// Create an instance of a deserializer.
    fn create() -> Self;

    /// Parse an object of type `T` from `data`.
    fn deserialize<T>(&mut self, data: &[u8]) -> AnyResult<T>
    where
        T: for<'de> Deserialize<'de>;
}

/// Deserializer for CSV-encoded data.
pub struct CsvDeserializerFromBytes {
    // CSV deserializer maintains some allocations across invocations,
    // so we keep an instance here.
    reader: csv::Reader<VecDeque<u8>>,
    // Byte record to read CSV records into.
    record: csv::ByteRecord,
}

impl DeserializerFromBytes for CsvDeserializerFromBytes {
    fn create() -> Self {
        CsvDeserializerFromBytes {
            reader: csv::ReaderBuilder::new()
                .has_headers(false)
                .flexible(true)
                .from_reader(VecDeque::new()),
            record: csv::ByteRecord::new(),
        }
    }
    fn deserialize<T>(&mut self, data: &[u8]) -> AnyResult<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        // Push new data to reader.
        self.reader.get_mut().extend(data.iter());
        self.reader.read_byte_record(&mut self.record)?;

        T::deserialize(&mut byte_record_deserializer(&self.record, None))
            .map_err(|e| anyhow!(e.to_string()))
    }
}

// Deserializer for JSON-encoded data.
pub struct JsonDeserializerFromBytes;

impl DeserializerFromBytes for JsonDeserializerFromBytes {
    fn create() -> Self {
        JsonDeserializerFromBytes
    }
    fn deserialize<T>(&mut self, data: &[u8]) -> AnyResult<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        T::deserialize(&mut serde_json::Deserializer::from_slice(data))
            .map_err(|e| anyhow!(e.to_string()))
    }
}

/// Maximal buffer size reused across clock cycles.
///
/// Input handles in this module use an internal buffer for input records.
/// We want to reuse the allocation across clock cycles, but we
/// limit the max amount of capacity we reuse so that a very large
/// transaction does not leave a huge unused memory buffer.
const MAX_REUSABLE_CAPACITY: usize = 100_000;

/// An input handle that deserializes values before pushing them to
/// a stream.
///
/// A trait for a type that wraps around an [`InputHandle`] and
/// pushes serialized data to an input stream.  The client of this trait
/// passes a deserializer object that provides access to a serialized data
/// value (e.g., in JSON or CSV format) to this API.  This data
/// gets deserialized into the strongly typed representation
/// expected by the input stream and pushed to the circuit using
/// the `InputHandle` API.
///
/// This trait is object-safe, i.e., it can be converted to `dyn DeScalarHandle`
/// and invoked using dynamic dispatch.  This allows choosing the input
/// format (encapsulated by the deserializer object) at runtime or even
/// switching between different formats dynamically.  Object safety is
/// achived by using the `erased_serde` crate that exposes an object-safe
/// deserializer API ([`erased_serde::Deserializer`]), so that `DeScalarHandle`
/// methods don't need to be parameterized by the deserializer type.
///
/// Note: this trait is useful for feeding scalar values to the circuit.
/// Use [`DeCollectionHandle`] for relational data.
pub trait ErasedDeScalarHandle: Send {
    /// Deserialize an input value and push it to the circuit via
    /// [`InputHandle::set_for_worker`].
    ///
    /// Returns an error if deserialization fails, i.e., the serialized
    /// representation is corrupted or does not match the type of the underlying
    /// input stream.
    fn set_for_worker(
        &self,
        worker: usize,
        deserializer: &mut dyn ErasedDeserializer,
    ) -> Result<(), EError>;

    /// Deserialize an input value and push it to the circuit via
    /// [`InputHandle::set_for_all`].
    ///
    /// Returns an error if deserialization fails, i.e., the serialized
    /// representation is corrupted or does not match the type of the underlying
    /// input stream.
    fn set_for_all(&self, deserializer: &mut dyn ErasedDeserializer) -> Result<(), EError>;

    /// Invoke [`InputHandle::clear_for_all`].
    fn clear_for_all(&self);

    /// Create a new handle connected to the same input stream.
    fn fork(&self) -> Box<dyn ErasedDeScalarHandle>;
}

#[derive(Clone)]
pub struct DeScalarHandleImpl<T> {
    handle: InputHandle<T>,
}

impl<T> DeScalarHandleImpl<T> {
    pub fn new(handle: InputHandle<T>) -> Self {
        Self { handle }
    }
}

impl<T> ErasedDeScalarHandle for DeScalarHandleImpl<T>
where
    T: Default + Send + Clone + for<'de> Deserialize<'de> + 'static,
{
    fn set_for_worker(
        &self,
        worker: usize,
        deserializer: &mut dyn ErasedDeserializer,
    ) -> Result<(), EError> {
        let val = deserialize::<T>(deserializer)?;
        self.handle.set_for_worker(worker, val);
        Ok(())
    }

    fn set_for_all(&self, deserializer: &mut dyn ErasedDeserializer) -> Result<(), EError> {
        let val = deserialize::<T>(deserializer)?;
        self.handle.set_for_all(val);
        Ok(())
    }

    fn clear_for_all(&self) {
        self.handle.clear_for_all()
    }

    fn fork(&self) -> Box<dyn ErasedDeScalarHandle> {
        Box::new(self.clone())
    }
}

pub struct DeZSetHandle<K, D, R> {
    handle: CollectionHandle<K, R>,
    phantom: PhantomData<D>,
}

impl<K, D, R> DeZSetHandle<K, D, R> {
    pub fn new(handle: CollectionHandle<K, R>) -> Self {
        Self {
            handle,
            phantom: PhantomData,
        }
    }
}

impl<K, D, R> DeCollectionHandle for DeZSetHandle<K, D, R>
where
    K: DBData + From<D>,
    D: for<'de> Deserialize<'de> + Send + 'static,
    R: DBWeight + ZRingValue,
{
    fn configure_deserializer(
        &self,
        record_format: RecordFormat,
    ) -> Result<Box<dyn DeCollectionStream>, ControllerError> {
        match record_format {
            RecordFormat::Csv => Ok(Box::new(
                DeZSetStream::<CsvDeserializerFromBytes, K, D, R>::new(self.handle.clone()),
            )),
            RecordFormat::Json => Ok(Box::new(
                DeZSetStream::<JsonDeserializerFromBytes, K, D, R>::new(self.handle.clone()),
            )),
        }
    }
}

/// An input handle that wraps a [`CollectionHandle<K, R>`](`CollectionHandle`)
/// returned by
/// [`RootCircuit::add_input_zset`](`dbsp::RootCircuit::add_input_zset`).
///
/// The [`insert`](`Self::insert`) method of this handle deserializes value
/// `d` of type `D`, converts it to value `k: K` using the `From` trait and
/// buffers a `(k, +1)` update for the underlying `CollectionHandle`.
///
/// The [`delete`](`Self::delete`) method of this handle buffers a `(k, -1)`
/// update for the underlying `CollectionHandle`.
pub struct DeZSetStream<De, K, D, R> {
    updates: Vec<(K, R)>,
    handle: CollectionHandle<K, R>,
    deserializer: De,
    phantom: PhantomData<D>,
}

impl<De, K, D, R> DeZSetStream<De, K, D, R>
where
    De: DeserializerFromBytes,
{
    pub fn new(handle: CollectionHandle<K, R>) -> Self {
        Self {
            updates: Vec::new(),
            handle,
            deserializer: De::create(),
            phantom: PhantomData,
        }
    }

    fn clear(&mut self) {
        if self.updates.capacity() > MAX_REUSABLE_CAPACITY {
            self.updates = Vec::new();
        }
    }
}

impl<De, K, D, R> DeCollectionStream for DeZSetStream<De, K, D, R>
where
    De: DeserializerFromBytes + Send + 'static,
    K: DBData + From<D>,
    D: for<'de> Deserialize<'de> + Send + 'static,
    R: DBWeight + ZRingValue,
{
    fn insert(&mut self, data: &[u8]) -> AnyResult<()> {
        let key = <K as From<D>>::from(self.deserializer.deserialize::<D>(data)?);

        self.updates.push((key, R::one()));
        Ok(())
    }

    fn delete(&mut self, data: &[u8]) -> AnyResult<()> {
        let key = <K as From<D>>::from(self.deserializer.deserialize::<D>(data)?);

        self.updates.push((key, R::one().neg()));
        Ok(())
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
        Box::new(Self::new(self.handle.clone()))
    }
}

pub struct DeSetHandle<K, D> {
    handle: UpsertHandle<K, bool>,
    phantom: PhantomData<D>,
}

impl<K, D> DeSetHandle<K, D> {
    pub fn new(handle: UpsertHandle<K, bool>) -> Self {
        Self {
            handle,
            phantom: PhantomData,
        }
    }
}

impl<K, D> DeCollectionHandle for DeSetHandle<K, D>
where
    K: DBData + From<D>,
    D: for<'de> Deserialize<'de> + Send + 'static,
{
    fn configure_deserializer(
        &self,
        record_format: RecordFormat,
    ) -> Result<Box<dyn DeCollectionStream>, ControllerError> {
        match record_format {
            RecordFormat::Csv => Ok(Box::new(
                DeSetStream::<CsvDeserializerFromBytes, K, D>::new(self.handle.clone()),
            )),
            RecordFormat::Json => Ok(Box::new(
                DeSetStream::<JsonDeserializerFromBytes, K, D>::new(self.handle.clone()),
            )),
        }
    }
}

/// An input handle that wraps a [`UpsertHandle<V, bool>`](`UpsertHandle`)
/// returned by
/// [`RootCircuit::add_input_set`](`dbsp::RootCircuit::add_input_set`).
///
/// The [`insert`](`Self::insert`) method of this handle deserializes value
/// `v` type `V` and buffers a `(v, true)` update for the underlying
/// `UpsertHandle`.
///
/// The [`delete`](`Self::delete`) method of this handle deserializes value
/// `v` type `V` and buffers a `(v, false)` update for the underlying
/// `UpsertHandle`.
pub struct DeSetStream<De, K, D> {
    updates: Vec<(K, bool)>,
    handle: UpsertHandle<K, bool>,
    deserializer: De,
    phantom: PhantomData<fn(D)>,
}

impl<De, K, D> DeSetStream<De, K, D>
where
    De: DeserializerFromBytes,
{
    pub fn new(handle: UpsertHandle<K, bool>) -> Self {
        Self {
            updates: Vec::new(),
            handle,
            deserializer: De::create(),
            phantom: PhantomData,
        }
    }
}

impl<De, K, D> DeCollectionStream for DeSetStream<De, K, D>
where
    De: DeserializerFromBytes + Send + 'static,
    K: DBData + From<D>,
    D: for<'de> Deserialize<'de> + Send + 'static,
{
    fn insert(&mut self, data: &[u8]) -> AnyResult<()> {
        let key = <K as From<D>>::from(self.deserializer.deserialize::<D>(data)?);

        self.updates.push((key, true));
        Ok(())
    }

    fn delete(&mut self, data: &[u8]) -> AnyResult<()> {
        let key = <K as From<D>>::from(self.deserializer.deserialize::<D>(data)?);

        self.updates.push((key, false));
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
        Box::new(Self::new(self.handle.clone()))
    }
}

pub struct DeMapHandle<K, V, F> {
    handle: UpsertHandle<K, Option<V>>,
    key_func: F,
}

impl<K, V, F> DeMapHandle<K, V, F> {
    pub fn new(handle: UpsertHandle<K, Option<V>>, key_func: F) -> Self {
        Self { handle, key_func }
    }
}

impl<K, V, F> DeCollectionHandle for DeMapHandle<K, V, F>
where
    K: DBData + for<'de> Deserialize<'de>,
    V: DBData + for<'de> Deserialize<'de>,
    F: Fn(&V) -> K + Clone + Send + 'static,
{
    fn configure_deserializer(
        &self,
        record_format: RecordFormat,
    ) -> Result<Box<dyn DeCollectionStream>, ControllerError> {
        match record_format {
            RecordFormat::Csv => Ok(Box::new(
                DeMapStream::<CsvDeserializerFromBytes, K, V, F>::new(
                    self.handle.clone(),
                    self.key_func.clone(),
                ),
            )),
            RecordFormat::Json => Ok(Box::new(
                DeMapStream::<JsonDeserializerFromBytes, K, V, F>::new(
                    self.handle.clone(),
                    self.key_func.clone(),
                ),
            )),
        }
    }
}

/// An input handle that wraps a [`UpsertHandle<K, Option<V>>`](`UpsertHandle`)
/// returned by
/// [`RootCircuit::add_input_map`](`dbsp::RootCircuit::add_input_map`).
///
/// The [`insert`](`Self::insert`) method of this handle deserializes value
/// `v` type `V` and buffers a `(key_func(v), Some(v))` update for the
/// underlying `UpsertHandle`, where `key_func: F` extracts key of type `K`
/// from value of type `V`.
///
/// The [`delete`](`Self::delete`) method of this handle deserializes value
/// `k` type `K` and buffers a `(k, None)` update for the underlying
/// `UpsertHandle`.
pub struct DeMapStream<De, K, V, F> {
    updates: Vec<(K, Option<V>)>,
    key_func: F,
    handle: UpsertHandle<K, Option<V>>,
    deserializer: De,
}

impl<De, K, V, F> DeMapStream<De, K, V, F>
where
    De: DeserializerFromBytes,
{
    pub fn new(handle: UpsertHandle<K, Option<V>>, key_func: F) -> Self {
        Self {
            updates: Vec::new(),
            key_func,
            handle,
            deserializer: De::create(),
        }
    }
}

impl<De, K, V, F> DeCollectionStream for DeMapStream<De, K, V, F>
where
    De: DeserializerFromBytes + Send + 'static,
    K: DBData + for<'de> Deserialize<'de>,
    V: DBData + for<'de> Deserialize<'de>,
    F: Fn(&V) -> K + Clone + Send + 'static,
{
    fn insert(&mut self, data: &[u8]) -> AnyResult<()> {
        let val = self.deserializer.deserialize::<V>(data)?;
        let key = (self.key_func)(&val);

        self.updates.push((key, Some(val)));
        Ok(())
    }

    fn delete(&mut self, data: &[u8]) -> AnyResult<()> {
        let key = self.deserializer.deserialize::<K>(data)?;

        self.updates.push((key, None));
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
        Box::new(Self::new(self.handle.clone(), self.key_func.clone()))
    }
}

#[cfg(test)]
mod test {
    use crate::{
        catalog::RecordFormat,
        static_compile::{
            DeMapHandle, DeScalarHandleImpl, DeSetHandle, DeZSetHandle, ErasedDeScalarHandle,
        },
        DeCollectionHandle,
    };
    use csv::WriterBuilder as CsvWriterBuilder;
    use csv_core::{ReadRecordResult, Reader as CsvReader};
    use dbsp::{
        algebra::F32, trace::Batch, DBSPHandle, OrdIndexedZSet, OrdZSet, OutputHandle, Runtime,
    };
    use erased_serde::Deserializer as ErasedDeserializer;
    use serde_json::{de::StrRead, to_string as to_json_string, Deserializer as JsonDeserializer};
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
    struct TestStruct {
        id: i64,
        s: String,
        b: bool,
        o: Option<F32>,
    }

    type InputHandles = (
        Box<dyn DeCollectionHandle>,
        Box<dyn DeCollectionHandle>,
        Box<dyn DeCollectionHandle>,
    );
    type OutputHandles = (
        OutputHandle<OrdZSet<TestStruct, isize>>,
        OutputHandle<OrdZSet<TestStruct, isize>>,
        OutputHandle<OrdIndexedZSet<i64, TestStruct, isize>>,
    );

    // Test circuit for DeScalarHandle.
    fn descalar_test_circuit(
        workers: usize,
    ) -> (
        DBSPHandle,
        Box<dyn ErasedDeScalarHandle>,
        OutputHandle<TestStruct>,
    ) {
        let (dbsp, (input_handle, output_handle)) = Runtime::init_circuit(workers, |circuit| {
            let (input, input_handle) = circuit.add_input_stream::<TestStruct>();
            let output_handle = input.output();

            Ok((input_handle, output_handle))
        })
        .unwrap();

        let input_handle = DeScalarHandleImpl::new(input_handle);

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

        for input in inputs.iter() {
            let input_json = to_json_string(input).unwrap();
            let mut deserializer = JsonDeserializer::new(StrRead::new(&input_json));
            let mut deserializer = <dyn ErasedDeserializer>::erase(&mut deserializer);

            input_handle.set_for_all(&mut deserializer).unwrap();
            dbsp.step().unwrap();

            let outputs = output_handle.take_from_all();
            assert!(outputs.iter().all(|x| x == input));
        }

        let input_handle_clone = input_handle.fork();

        for (w, input) in inputs.iter().enumerate() {
            let input_json = to_json_string(input).unwrap();
            let mut deserializer = JsonDeserializer::new(StrRead::new(&input_json));
            let mut deserializer = <dyn ErasedDeserializer>::erase(&mut deserializer);

            input_handle_clone
                .set_for_worker(w % NUM_WORKERS, &mut deserializer)
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
                let (zset, zset_handle) = circuit.add_input_zset::<TestStruct, isize>();
                let (set, set_handle) = circuit.add_input_set::<TestStruct, isize>();
                let (map, map_handle) = circuit.add_input_map::<i64, TestStruct, isize>();

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
        let de_map = DeMapHandle::new(map_input, |test_struct: &TestStruct| test_struct.id);

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

        let zset = OrdZSet::from_tuples(
            (),
            inputs.iter().map(|v| (v.clone(), 1)).collect::<Vec<_>>(),
        );
        let map = <OrdIndexedZSet<i64, TestStruct, isize, usize>>::from_tuples(
            (),
            inputs
                .iter()
                .map(|v| ((v.id, v.clone()), 1isize))
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
            .configure_deserializer(RecordFormat::Json)
            .unwrap();
        let mut set_input = input_handles
            .1
            .configure_deserializer(RecordFormat::Json)
            .unwrap();
        let mut map_input = input_handles
            .2
            .configure_deserializer(RecordFormat::Json)
            .unwrap();

        let zset_output = &output_handles.0;
        let set_output = &output_handles.1;
        let map_output = &output_handles.2;

        let zset = OrdZSet::from_tuples(
            (),
            inputs.iter().map(|v| (v.clone(), -1)).collect::<Vec<_>>(),
        );
        let map = <OrdIndexedZSet<i64, TestStruct, isize, usize>>::from_tuples(
            (),
            inputs
                .iter()
                .map(|v| ((v.id, v.clone()), -1isize))
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
