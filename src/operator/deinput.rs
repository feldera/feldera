use crate::{algebra::ZRingValue, CollectionHandle, DBData, DBWeight, InputHandle, UpsertHandle};
use erased_serde::{deserialize, Deserializer as ErasedDeserializer, Error as EError};
use serde::Deserialize;

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
pub trait DeScalarHandle: Send {
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
    fn fork(&self) -> Box<dyn DeScalarHandle>;
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

impl<T> DeScalarHandle for DeScalarHandleImpl<T>
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

    fn fork(&self) -> Box<dyn DeScalarHandle> {
        Box::new(self.clone())
    }
}

/// An input handle that deserializes records before pushing them to a
/// stream.
///
/// A trait for a type that wraps a [`CollectionHandle`] or
/// an [`UpsertHandle`] and pushes serialized relational data to the
/// associated input stream record-by-record.  The client passes a
/// deserializer object that provides access to a serialized data
/// record (e.g., in JSON or CSV format) to [`insert`](`Self::insert`)
/// and [`delete`](`Self::delete`) methods.  The record gets deserialized
/// into the strongly typed representation expected by the input stream
/// and gets buffered inside the handle.  The [`flush`](`Self::flush`)
/// method pushes all buffered data to the underlying
/// [`CollectionHandle`] or [`UpsertHandle`].
///
/// The exact serialized type passed to `insert` and `delete` methods
/// depends on the underlying handle. See [`DeZSetHandle`], [`DeSetHandle`],
/// and [`DeMapHandle`] documentation for details.
///
/// This trait is object-safe, i.e., it can be converted to
/// `dyn DeCollectionHandle` and invoked using dynamic dispatch.  This allows
/// choosing the input format (encapsulated by the deserializer object) at
/// runtime or even switching between different formats dynamically.  Object
/// safety is achived by using the `erased_serde` crate that exposes an
/// object-safe deserializer API ([`erased_serde::Deserializer`]), so that
/// `DeCollectionHandle` methods don't need to be parameterized by the
/// deserializer type.
pub trait DeCollectionHandle: Send {
    /// Buffer a new insert update.
    ///
    /// The `deserializer` argument wraps a single serialized record.
    /// The value gets deserialized and pushed to the underlying input stream
    /// handle as an insert update.
    ///
    /// Returns an error if deserialization fails, i.e., the serialized
    /// representation is corrupted or does not match the value type of
    /// the underlying input stream.
    ///
    /// See [`DeZSetHandle`], [`DeSetHandle`], and [`DeMapHandle`]
    /// documentation for details.
    fn insert(&mut self, deserializer: &mut dyn ErasedDeserializer) -> Result<(), EError>;

    /// Buffer a new delete update.
    ///
    /// The `deserializer` argument wraps a single serialized record whose
    /// type depends on the underlying input stream: streams created by
    /// [`Circuit::add_input_zset`](`crate::Circuit::add_input_zset`)
    /// and [`Circuit::add_input_set`](`crate::Circuit::add_input_set`)
    /// methods support deletion by value, hence the serialized record must
    /// match the value type of the stream.  Streams created with
    /// [`Circuit::add_input_map`](`crate::Circuit::add_input_map`)
    /// support deletion by key, so the serialized record must match the key
    /// type of the stream.
    ///
    /// The record gets deserialized and pushed to the underlying input stream
    /// handle as a delete update.
    ///
    /// Returns an error if deserialization fails, i.e., the serialized
    /// representation is corrupted or does not match the value or key
    /// type of the underlying input stream.
    ///
    /// See [`DeZSetHandle`], [`DeSetHandle`], and [`DeMapHandle`]
    /// documentation for details.
    fn delete(&mut self, deserializer: &mut dyn ErasedDeserializer) -> Result<(), EError>;

    /// Reserve space for at least `reservation` more updates in the
    /// internal input buffer.
    ///
    /// Reservations are not required but can be used when the number
    /// of inputs is known ahead of time to reduce reallocations.
    fn reserve(&mut self, reservation: usize);

    /// Push all buffered updates to the underlying input stream handle.
    ///
    /// Flushed updates will be pushed to the stream during the next call
    /// to [`DBSPHandle::step`](`crate::DBSPHandle::step`).  `flush` can
    /// be called multiple times between two subsequent `step`s.  Every
    /// `flush` call adds new updates to the previously flushed updates.
    ///
    /// Updates queued after the last `flush` remain buffered in the handle
    /// until the next `flush` or `clear_buffer` call or until the handle
    /// is destroyed.
    fn flush(&mut self);

    /// Clear all buffered updates.
    ///
    /// Clears updates pushed to the handle after the last `flush`.
    /// Flushed updates remain queued at the underlying input handle.
    // TODO: add another method to invoke `CollectionHandle::clear_input`?
    fn clear_buffer(&mut self);

    /// Create a new handle connected to the same input stream.
    ///
    /// The new handle will use its own input buffer, but shares the
    /// underlying input stream handle with the original handle.
    fn fork(&self) -> Box<dyn DeCollectionHandle>;
}

/// An input handle that wraps a [`CollectionHandle<V, R>`](`CollectionHandle`)
/// returned by [`Circuit::add_input_zset`](`crate::Circuit::add_input_zset`).
///
/// The [`insert`](`Self::insert`) method of this handle deserializes value
/// `v` type `V` and buffers a `(v, +1)` update for the underlying
/// `CollectionHandle`.
///
/// The [`delete`](`Self::delete`) method of this handle deserializes value
/// `v` type `V` and buffers a `(v, -1)` update for the underlying
/// `CollectionHandle`.
pub struct DeZSetHandle<K, R> {
    updates: Vec<(K, R)>,
    handle: CollectionHandle<K, R>,
}

impl<K, R> DeZSetHandle<K, R> {
    pub fn new(handle: CollectionHandle<K, R>) -> Self {
        Self {
            updates: Vec::new(),
            handle,
        }
    }

    fn clear(&mut self) {
        if self.updates.capacity() > MAX_REUSABLE_CAPACITY {
            self.updates = Vec::new();
        }
    }
}

impl<K, R> DeCollectionHandle for DeZSetHandle<K, R>
where
    K: DBData + for<'de> Deserialize<'de>,
    R: DBWeight + ZRingValue,
{
    fn insert(&mut self, deserializer: &mut dyn ErasedDeserializer) -> Result<(), EError> {
        let key = deserialize::<K>(deserializer)?;

        self.updates.push((key, R::one()));
        Ok(())
    }

    fn delete(&mut self, deserializer: &mut dyn ErasedDeserializer) -> Result<(), EError> {
        let key = deserialize::<K>(deserializer)?;

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

    fn fork(&self) -> Box<dyn DeCollectionHandle> {
        Box::new(Self::new(self.handle.clone()))
    }
}

/// An input handle that wraps a [`UpsertHandle<V, bool>`](`UpsertHandle`)
/// returned by [`Circuit::add_input_set`](`crate::Circuit::add_input_set`).
///
/// The [`insert`](`Self::insert`) method of this handle deserializes value
/// `v` type `V` and buffers a `(v, true)` update for the underlying
/// `UpsertHandle`.
///
/// The [`delete`](`Self::delete`) method of this handle deserializes value
/// `v` type `V` and buffers a `(v, false)` update for the underlying
/// `UpsertHandle`.
pub struct DeSetHandle<K> {
    updates: Vec<(K, bool)>,
    handle: UpsertHandle<K, bool>,
}

impl<K> DeSetHandle<K> {
    pub fn new(handle: UpsertHandle<K, bool>) -> Self {
        Self {
            updates: Vec::new(),
            handle,
        }
    }
}

impl<K> DeCollectionHandle for DeSetHandle<K>
where
    K: DBData + for<'de> Deserialize<'de>,
{
    fn insert(&mut self, deserializer: &mut dyn ErasedDeserializer) -> Result<(), EError> {
        let key = deserialize::<K>(deserializer)?;

        self.updates.push((key, true));
        Ok(())
    }

    fn delete(&mut self, deserializer: &mut dyn ErasedDeserializer) -> Result<(), EError> {
        let key = deserialize::<K>(deserializer)?;

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

    fn fork(&self) -> Box<dyn DeCollectionHandle> {
        Box::new(Self::new(self.handle.clone()))
    }
}

/// An input handle that wraps a [`UpsertHandle<K, Option<V>>`](`UpsertHandle`)
/// returned by [`Circuit::add_input_map`](`crate::Circuit::add_input_map`).
///
/// The [`insert`](`Self::insert`) method of this handle deserializes value
/// `v` type `V` and buffers a `(key_func(v), Some(v))` update for the
/// underlying `UpsertHandle`, where `key_func: F` extracts key of type `K`
/// from value of type `V`.
///
/// The [`delete`](`Self::delete`) method of this handle deserializes value
/// `k` type `K` and buffers a `(k, None)` update for the underlying
/// `UpsertHandle`.
pub struct DeMapHandle<K, V, F> {
    updates: Vec<(K, Option<V>)>,
    key_func: F,
    handle: UpsertHandle<K, Option<V>>,
}

impl<K, V, F> DeMapHandle<K, V, F> {
    pub fn new(handle: UpsertHandle<K, Option<V>>, key_func: F) -> Self {
        Self {
            updates: Vec::new(),
            key_func,
            handle,
        }
    }
}

impl<K, V, F> DeCollectionHandle for DeMapHandle<K, V, F>
where
    K: DBData + for<'de> Deserialize<'de>,
    V: DBData + for<'de> Deserialize<'de>,
    F: Fn(&V) -> K + Clone + Send + 'static,
{
    fn insert(&mut self, deserializer: &mut dyn ErasedDeserializer) -> Result<(), EError> {
        let val = deserialize::<V>(deserializer)?;
        let key = (self.key_func)(&val);

        self.updates.push((key, Some(val)));
        Ok(())
    }

    fn delete(&mut self, deserializer: &mut dyn ErasedDeserializer) -> Result<(), EError> {
        let key = deserialize::<K>(deserializer)?;

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

    fn fork(&self) -> Box<dyn DeCollectionHandle> {
        Box::new(Self::new(self.handle.clone(), self.key_func.clone()))
    }
}

#[cfg(test)]
#[cfg(feature = "with-csv")]
mod test {
    use crate::{
        algebra::F32,
        operator::{
            DeCollectionHandle, DeMapHandle, DeScalarHandle, DeScalarHandleImpl, DeSetHandle,
            DeZSetHandle,
        },
        trace::Batch,
        DBSPHandle, OrdIndexedZSet, OrdZSet, OutputHandle, Runtime,
    };
    use bincode::{Decode, Encode};
    use csv::{string_record_deserializer, Reader as CsvReader, Writer as CsvWriter};
    use erased_serde::Deserializer as ErasedDeserializer;
    use serde::{Deserialize, Serialize};
    use serde_json::{de::StrRead, to_string as to_json_string, Deserializer as JsonDeserializer};
    use size_of::SizeOf;
    use std::hash::Hash;

    const NUM_WORKERS: usize = 4;

    #[derive(
        Clone,
        Debug,
        Encode,
        Decode,
        Default,
        Hash,
        PartialEq,
        Eq,
        PartialOrd,
        Ord,
        Deserialize,
        Serialize,
        SizeOf,
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
        Box<dyn DeScalarHandle>,
        OutputHandle<TestStruct>,
    ) {
        let (dbsp, (input_handle, output_handle)) = Runtime::init_circuit(workers, |circuit| {
            let (input, input_handle) = circuit.add_input_stream::<TestStruct>();
            let output_handle = input.output();

            (input_handle, output_handle)
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

                (
                    (zset_handle, zset_output),
                    (set_handle, set_output),
                    (map_handle, map_output),
                )
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
        input_handles: &mut InputHandles,
        output_handles: &OutputHandles,
        inputs: &[TestStruct],
    ) {
        let zset_handle = &mut input_handles.0;
        let set_handle = &mut input_handles.1;
        let map_handle = &mut input_handles.2;

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

        zset_handle.reserve(inputs.len());
        set_handle.reserve(1);
        map_handle.reserve(0);

        // Serialize `inputs` as CSV.
        let mut csv_bytes: Vec<u8> = Vec::new();
        let mut csv_writer = CsvWriter::from_writer(&mut csv_bytes);
        for input in inputs.iter() {
            csv_writer.serialize(input).unwrap();
        }
        let _ = csv_writer.into_inner().unwrap();
        let csv_string = String::from_utf8(csv_bytes).unwrap();

        // println!("csv:\n{}", csv_string);

        // CSV reader iterates over CSV records.
        let mut csv_reader = CsvReader::from_reader(csv_string.as_bytes());

        for input in csv_reader.records() {
            let record = input.unwrap();
            // Wrap CSV record in erased deserializer.
            let mut deserializer = string_record_deserializer(&record, None);
            let mut deserializer = <dyn ErasedDeserializer>::erase(&mut deserializer);
            zset_handle.insert(&mut deserializer).unwrap();

            let mut deserializer = string_record_deserializer(&record, None);
            let mut deserializer = <dyn ErasedDeserializer>::erase(&mut deserializer);
            set_handle.insert(&mut deserializer).unwrap();

            let mut deserializer = string_record_deserializer(&record, None);
            let mut deserializer = <dyn ErasedDeserializer>::erase(&mut deserializer);
            map_handle.insert(&mut deserializer).unwrap();
        }

        zset_handle.flush();
        set_handle.flush();
        map_handle.flush();

        dbsp.step().unwrap();

        assert_eq!(zset_output.consolidate(), zset);
        assert_eq!(set_output.consolidate(), zset);
        assert_eq!(map_output.consolidate(), map);
    }

    // Delete `inputs` in JSON format.
    fn delete_json(
        dbsp: &mut DBSPHandle,
        input_handles: &mut InputHandles,
        output_handles: &OutputHandles,
        inputs: &[TestStruct],
    ) {
        let zset_input = &mut input_handles.0;
        let set_input = &mut input_handles.1;
        let map_input = &mut input_handles.2;

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

            let mut deserializer = JsonDeserializer::new(StrRead::new(&input));
            let mut deserializer = <dyn ErasedDeserializer>::erase(&mut deserializer);
            zset_input.delete(&mut deserializer).unwrap();
            zset_input.flush();

            let mut deserializer = JsonDeserializer::new(StrRead::new(&input));
            let mut deserializer = <dyn ErasedDeserializer>::erase(&mut deserializer);
            set_input.delete(&mut deserializer).unwrap();
            set_input.flush();

            let input_id = to_json_string(&id).unwrap();
            let mut deserializer = JsonDeserializer::new(StrRead::new(&input_id));
            let mut deserializer = <dyn ErasedDeserializer>::erase(&mut deserializer);
            map_input.delete(&mut deserializer).unwrap();
            map_input.flush();
        }

        dbsp.step().unwrap();

        assert_eq!(zset_output.consolidate(), zset);
        assert_eq!(set_output.consolidate(), zset);
        assert_eq!(map_output.consolidate(), map);
    }

    #[test]
    fn test_collection() {
        let (mut dbsp, mut input_handles, output_handles) = decollection_test_circuit(NUM_WORKERS);

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

        insert_csv(&mut dbsp, &mut input_handles, &output_handles, &inputs);

        let mut input_handles_clone = (
            input_handles.0.fork(),
            input_handles.1.fork(),
            input_handles.2.fork(),
        );
        delete_json(
            &mut dbsp,
            &mut input_handles_clone,
            &output_handles,
            &inputs,
        );

        dbsp.kill().unwrap();
    }
}
