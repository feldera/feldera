use crate::{
    catalog::{NeighborhoodEntry, OutputCollectionHandles, SerCollectionHandle},
    static_compile::{DeScalarHandle, DeScalarHandleImpl},
    Catalog, DeserializeWithContext,
};
use dbsp::{
    algebra::ZRingValue,
    operator::{DelayedFeedback, FilterMap, NeighborhoodDescr},
    CollectionHandle, DBData, DBWeight, OrdIndexedZSet, RootCircuit, Stream, UpsertHandle, ZSet,
};
use serde::Serialize;

use super::{
    DeMapHandle, DeSetHandle, DeZSetHandle, SerCollectionHandleImpl, SqlDeserializerConfig,
};

impl Catalog {
    /// Add an input stream of Z-sets to the catalog.
    ///
    /// Adds a `DeCollectionHandle` to the catalog, which will deserialize
    /// input records into type `D` before converting them to `Z::Key` using
    /// the `From` trait.
    pub fn register_input_zset<Z, D>(
        &mut self,
        name: &str,
        stream: Stream<RootCircuit, Z>,
        handle: CollectionHandle<Z::Key, Z::R>,
    ) where
        D: for<'de> DeserializeWithContext<'de, SqlDeserializerConfig>
            + Serialize
            + From<Z::Key>
            + Clone
            + Send
            + 'static,
        Z: ZSet + Send + Sync,
        Z::R: ZRingValue + Into<i64> + Sync,
        Z::Key: Serialize + Sync + From<D>,
    {
        self.register_input_collection_handle(name, DeZSetHandle::new(handle));

        // Inputs are also outputs.
        self.register_output_zset(name, stream);
    }

    /// Add an input stream created using `add_input_set` to catalog.
    ///
    /// Adds a `DeCollectionHandle` to the catalog, which will deserialize
    /// input records into type `D` before converting them to `Z::Key` using
    /// the `From` trait.
    pub fn register_input_set<Z, D>(
        &mut self,
        name: &str,
        stream: Stream<RootCircuit, Z>,
        handle: UpsertHandle<Z::Key, bool>,
    ) where
        D: for<'de> DeserializeWithContext<'de, SqlDeserializerConfig>
            + Serialize
            + From<Z::Key>
            + Clone
            + Send
            + 'static,
        Z: ZSet + Send + Sync,
        Z::R: ZRingValue + Into<i64> + Sync,
        Z::Key: Serialize + Sync + From<D>,
    {
        self.register_input_collection_handle(name, DeSetHandle::new(handle));

        // Inputs are also outputs.
        self.register_output_zset(name, stream);
    }

    /// Register an input handle created using `add_input_map`.
    ///
    /// Elements are inserted by value and deleted by key.  On insert, the handle uses `key_func`
    /// to extract the key from the value.
    ///
    /// # Generics
    ///
    /// * `K` - Key type of the input collection.
    /// * `KD` - Key type in the input byte stream.  Keys will get deserialized into instances of `KD` and then converted to `K`.
    /// * `V` - value type of the input collection.
    /// * `VD` - Value type in the input byte stream.  Values will get deserialized into instances of `VD` and then converted to `K`.
    pub fn register_input_map<K, KD, V, VD, R, F>(
        &mut self,
        name: &str,
        stream: Stream<RootCircuit, OrdIndexedZSet<K, V, R>>,
        handle: UpsertHandle<K, Option<V>>,
        key_func: F,
    ) where
        F: Fn(&V) -> K + Clone + Send + Sync + 'static,
        KD: for<'de> DeserializeWithContext<'de, SqlDeserializerConfig>
            + Serialize
            + From<K>
            + Clone
            + Send
            + 'static,
        VD: for<'de> DeserializeWithContext<'de, SqlDeserializerConfig>
            + Serialize
            + From<V>
            + Clone
            + Send
            + 'static,
        R: DBWeight + ZRingValue + Into<i64> + Sync,
        K: DBData + Serialize + Sync + Default + From<KD>,
        V: DBData + Serialize + Sync + From<VD> + Default,
    {
        self.register_input_collection_handle(name, DeMapHandle::new(handle, key_func.clone()));

        // Inputs are also outputs.
        self.register_output_map(name, stream, key_func);
    }

    /// Add an output stream of Z-sets to the catalog.
    pub fn register_output_zset<Z, D>(&mut self, name: &str, stream: Stream<RootCircuit, Z>)
    where
        D: for<'de> DeserializeWithContext<'de, SqlDeserializerConfig>
            + Serialize
            + From<Z::Key>
            + Clone
            + Send
            + 'static,
        Z: ZSet + Send + Sync,
        Z::R: ZRingValue + Into<i64> + Sync,
        Z::Key: Serialize + Sync + From<D>,
    {
        let circuit = stream.circuit();

        // Create handle for the stream itself.
        let delta_handle = stream.output();

        // Improve the odds that `integrate_trace` below reuses the trace of `stream`
        // if one exists.
        let stream = stream.try_sharded_version();

        // Create handles for the neighborhood query.
        let (neighborhood_descr_stream, neighborhood_descr_handle) =
            circuit.add_input_stream::<(bool, Option<NeighborhoodDescr<D, ()>>)>();
        let neighborhood_stream = {
            // Create a feedback loop to latch the latest neighborhood descriptor
            // when `reset=true`.
            let feedback =
                <DelayedFeedback<RootCircuit, Option<NeighborhoodDescr<Z::Key, ()>>>>::new(
                    stream.circuit(),
                );
            let new_neighborhood =
                feedback
                    .stream()
                    .apply2(&neighborhood_descr_stream, |old, (reset, new)| {
                        if *reset {
                            // Convert anchor of type `D` into `Z::Key`.
                            new.clone().map(|new| {
                                NeighborhoodDescr::new(
                                    new.anchor.map(From::from),
                                    (),
                                    new.before,
                                    new.after,
                                )
                            })
                        } else {
                            old.clone()
                        }
                    });
            feedback.connect(&new_neighborhood);
            stream.neighborhood(&new_neighborhood)
        };

        // Neighborhood delta stream.
        let neighborhood_handle = neighborhood_stream.output();

        // Neighborhood snapshot stream.  The integral computation
        // is essentially free thanks to stream caching.
        let neighborhood_snapshot_stream = neighborhood_stream.integrate();
        let neighborhood_snapshot_handle = neighborhood_snapshot_stream
            .output_guarded(&neighborhood_descr_stream.apply(|(reset, _descr)| *reset));

        // Handle for the quantiles query.
        let (num_quantiles_stream, num_quantiles_handle) = circuit.add_input_stream::<usize>();

        // Output of the quantiles query, only produced when `num_quantiles>0`.
        let quantiles_stream = stream
            .integrate_trace()
            .stream_key_quantiles(&num_quantiles_stream);
        let quantiles_handle = quantiles_stream
            .output_guarded(&num_quantiles_stream.apply(|num_quantiles| *num_quantiles > 0));

        let handles = OutputCollectionHandles {
            delta_handle: Box::new(<SerCollectionHandleImpl<_, D, ()>>::new(delta_handle))
                as Box<dyn SerCollectionHandle>,

            neighborhood_descr_handle: Some(Box::new(DeScalarHandleImpl::new(
                neighborhood_descr_handle,
                |x| x,
            )) as Box<dyn DeScalarHandle>),
            neighborhood_handle: Some(Box::new(
                <SerCollectionHandleImpl<_, NeighborhoodEntry<D>, ()>>::new(neighborhood_handle),
            ) as Box<dyn SerCollectionHandle>),
            neighborhood_snapshot_handle: Some(Box::new(<SerCollectionHandleImpl<
                _,
                NeighborhoodEntry<D>,
                (),
            >>::new(
                neighborhood_snapshot_handle
            )) as Box<dyn SerCollectionHandle>),

            num_quantiles_handle: Some(num_quantiles_handle),
            quantiles_handle: Some(Box::new(<SerCollectionHandleImpl<_, D, ()>>::new(
                quantiles_handle,
            )) as Box<dyn SerCollectionHandle>),
        };

        self.output_batch_handles.insert(name.to_owned(), handles);
    }

    /// Add an output stream that carries updates to an indexed Z-set that
    /// behaves like a map (i.e., has exactly one key with weight 1 per value)
    /// to the catalog.
    ///
    /// Creates neighborhood and quantiles circuits.  Drops the key component
    /// of the `(key,value)` tuple, so that delta, neighborhood, and quantiles
    /// streams contain values only.  Clients, e.g., the web console, can
    /// work with maps and z-sets in the same way.
    pub fn register_output_map<K, KD, V, VD, R, F>(
        &mut self,
        name: &str,
        stream: Stream<RootCircuit, OrdIndexedZSet<K, V, R>>,
        key_func: F,
    ) where
        F: Fn(&V) -> K + Clone + Send + Sync + 'static,
        KD: for<'de> DeserializeWithContext<'de, SqlDeserializerConfig>
            + Serialize
            + From<K>
            + Clone
            + Send
            + 'static,
        VD: for<'de> DeserializeWithContext<'de, SqlDeserializerConfig>
            + Serialize
            + From<V>
            + Clone
            + Send
            + 'static,
        R: DBWeight + ZRingValue + Into<i64> + Sync,
        K: DBData + Serialize + Send + Sync + From<KD> + Default,
        V: DBData + Serialize + Send + Sync + From<VD> + Default,
    {
        let circuit = stream.circuit();

        // Create handle for the stream itself.
        let delta_handle = stream.map(|(_k, v)| v.clone()).output();

        // Improve the odds that `integrate_trace` below reuses the trace of `stream`
        // if one exists.
        let stream = stream.try_sharded_version();

        // Create handles for the neighborhood query.
        let (neighborhood_descr_stream, neighborhood_descr_handle) =
            circuit.add_input_stream::<(bool, Option<NeighborhoodDescr<VD, ()>>)>();
        let neighborhood_stream = {
            // Create a feedback loop to latch the latest neighborhood descriptor
            // when `reset=true`.
            let feedback = <DelayedFeedback<RootCircuit, Option<NeighborhoodDescr<K, V>>>>::new(
                stream.circuit(),
            );
            let new_neighborhood = feedback.stream().apply2(
                &neighborhood_descr_stream,
                move |old: &Option<NeighborhoodDescr<K, V>>, (reset, new)| {
                    if *reset {
                        let key_func = key_func.clone();
                        // Convert anchor of type `(VD, ())` into `(Z::Key, Z::Val)`.
                        new.clone().map(move |new| {
                            let val = new.anchor.map(V::from);
                            let key = val.as_ref().map(key_func);
                            NeighborhoodDescr::new(
                                key,
                                val.unwrap_or_default(),
                                new.before,
                                new.after,
                            )
                        })
                    } else {
                        old.clone()
                    }
                },
            );
            feedback.connect(&new_neighborhood);
            stream.neighborhood(&new_neighborhood)
        };

        // Neighborhood delta stream.
        let neighborhood_handle = neighborhood_stream
            .map(|(idx, (_k, v))| (*idx, (v.clone(), ())))
            .output();

        // Neighborhood snapshot stream.  The integral computation
        // is essentially free thanks to stream caching.
        let neighborhood_snapshot_stream = neighborhood_stream.integrate();
        let neighborhood_snapshot_handle = neighborhood_snapshot_stream
            .map(|(idx, (_k, v))| (*idx, (v.clone(), ())))
            .output_guarded(&neighborhood_descr_stream.apply(|(reset, _descr)| *reset));

        // Handle for the quantiles query.
        let (num_quantiles_stream, num_quantiles_handle) = circuit.add_input_stream::<usize>();

        // Output of the quantiles query, only produced when `num_quantiles>0`.
        let quantiles_stream = stream
            .integrate_trace()
            .stream_unique_key_val_quantiles(&num_quantiles_stream);
        let quantiles_handle = quantiles_stream
            .map(|(_k, v)| v.clone())
            .output_guarded(&num_quantiles_stream.apply(|num_quantiles| *num_quantiles > 0));

        let handles = OutputCollectionHandles {
            delta_handle: Box::new(<SerCollectionHandleImpl<_, VD, ()>>::new(delta_handle))
                as Box<dyn SerCollectionHandle>,

            neighborhood_descr_handle: Some(Box::new(DeScalarHandleImpl::new(
                neighborhood_descr_handle,
                |x| x,
            )) as Box<dyn DeScalarHandle>),
            neighborhood_handle: Some(Box::new(<SerCollectionHandleImpl<
                _,
                NeighborhoodEntry<VD>,
                (),
            >>::new(neighborhood_handle))
                as Box<dyn SerCollectionHandle>),
            neighborhood_snapshot_handle: Some(Box::new(<SerCollectionHandleImpl<
                _,
                NeighborhoodEntry<VD>,
                (),
            >>::new(
                neighborhood_snapshot_handle
            )) as Box<dyn SerCollectionHandle>),

            num_quantiles_handle: Some(num_quantiles_handle),
            quantiles_handle: Some(Box::new(<SerCollectionHandleImpl<_, VD, ()>>::new(
                quantiles_handle,
            )) as Box<dyn SerCollectionHandle>),
        };

        self.output_batch_handles.insert(name.to_owned(), handles);
    }
}

#[cfg(test)]
mod test {
    use std::{io::Write, ops::Deref};

    use crate::{
        catalog::{OutputCollectionHandles, RecordFormat},
        format::JsonFlavor,
        test::TestStruct,
        Catalog, CircuitCatalog, SerBatch,
    };
    use dbsp::Runtime;

    const RECORD_FORMAT: RecordFormat = RecordFormat::Json(JsonFlavor::Default);

    fn batch_to_json(batch: &dyn SerBatch) -> String {
        let mut cursor = batch.cursor(RECORD_FORMAT.clone()).unwrap();
        let mut result = Vec::new();

        while cursor.key_valid() {
            write!(&mut result, "{}: ", cursor.weight()).unwrap();
            cursor.serialize_key(&mut result).unwrap();
            result.push(b'\n');
            cursor.step_key();
        }

        String::from_utf8(result).unwrap()
    }

    fn set_num_quantiles(output_handles: &OutputCollectionHandles, num_quantiles: usize) {
        output_handles
            .num_quantiles_handle
            .as_ref()
            .unwrap()
            .set_for_all(num_quantiles);
    }

    fn get_quantiles(output_handles: &OutputCollectionHandles) -> String {
        batch_to_json(
            output_handles
                .quantiles_handle
                .as_ref()
                .unwrap()
                .consolidate()
                .deref(),
        )
    }

    fn get_hood(output_handles: &OutputCollectionHandles) -> String {
        batch_to_json(
            output_handles
                .neighborhood_handle
                .as_ref()
                .unwrap()
                .consolidate()
                .deref(),
        )
    }

    fn set_neighborhood_descr<T: serde::Serialize>(
        output_handles: &OutputCollectionHandles,
        anchor: &T,
        before: usize,
        after: usize,
    ) {
        let anchor = serde_json::to_string(anchor).unwrap();
        output_handles
            .neighborhood_descr_handle
            .as_ref()
            .unwrap()
            .configure_deserializer(RECORD_FORMAT)
            .unwrap()
            .set_for_all(
                format!(r#"[true, {{"anchor":{anchor},"before":{before},"after":{after}}}]"#)
                    .as_bytes(),
            )
            .unwrap();
    }

    #[test]
    fn catalog_map_handle_test() {
        let (mut circuit, catalog) = Runtime::init_circuit(4, |circuit| {
            let mut catalog = Catalog::new();
            let (input, hinput) = circuit.add_input_map::<u32, TestStruct, i32>();

            catalog.register_input_map::<u32, u32, TestStruct, TestStruct, _, _>(
                "input_map",
                input.clone(),
                hinput,
                |test_struct| test_struct.id,
            );

            Ok(catalog)
        })
        .unwrap();

        let input_map_handle = catalog.input_collection_handle("input_map").unwrap();
        let mut input_stream_handle = input_map_handle
            .configure_deserializer(RECORD_FORMAT.clone())
            .unwrap();

        let output_stream_handles = catalog.output_handles("input_map").unwrap();

        // Step 1: insert a couple of values.

        input_stream_handle
            .insert(br#"{"id": 1, "b": true, "s": "1"}"#)
            .unwrap();
        input_stream_handle
            .insert(br#"{"id": 2, "b": true, "s": "2"}"#)
            .unwrap();
        input_stream_handle.flush();

        set_num_quantiles(output_stream_handles, 5);
        set_neighborhood_descr(output_stream_handles, &TestStruct::default(), 5, 5);

        circuit.step().unwrap();

        let delta = batch_to_json(output_stream_handles.delta_handle.consolidate().deref());
        assert_eq!(
            delta,
            r#"1: {"id":1,"b":true,"i":null,"s":"1"}
1: {"id":2,"b":true,"i":null,"s":"2"}
"#
        );

        let quantiles = get_quantiles(output_stream_handles);
        assert_eq!(
            quantiles,
            r#"1: {"id":1,"b":true,"i":null,"s":"1"}
1: {"id":2,"b":true,"i":null,"s":"2"}
"#
        );

        let hood = get_hood(output_stream_handles);
        assert_eq!(
            hood,
            r#"1: {"index":0,"key":{"id":1,"b":true,"i":null,"s":"1"}}
1: {"index":1,"key":{"id":2,"b":true,"i":null,"s":"2"}}
"#
        );

        // Step 2: replace an entry.

        input_stream_handle
            .insert(br#"{"id": 1, "b": true, "s": "1-modified"}"#)
            .unwrap();
        input_stream_handle.flush();

        set_num_quantiles(output_stream_handles, 5);

        circuit.step().unwrap();

        let delta = batch_to_json(output_stream_handles.delta_handle.consolidate().deref());
        assert_eq!(
            delta,
            r#"-1: {"id":1,"b":true,"i":null,"s":"1"}
1: {"id":1,"b":true,"i":null,"s":"1-modified"}
"#
        );

        let quantiles = get_quantiles(output_stream_handles);
        assert_eq!(
            quantiles,
            r#"1: {"id":1,"b":true,"i":null,"s":"1-modified"}
1: {"id":2,"b":true,"i":null,"s":"2"}
"#
        );

        // Step 3: delete an entry.

        input_stream_handle.delete(br#"2"#).unwrap();
        input_stream_handle.flush();
        set_num_quantiles(output_stream_handles, 5);

        circuit.step().unwrap();

        let delta = batch_to_json(output_stream_handles.delta_handle.consolidate().deref());
        assert_eq!(
            delta,
            r#"-1: {"id":2,"b":true,"i":null,"s":"2"}
"#
        );

        let quantiles = get_quantiles(output_stream_handles);
        assert_eq!(
            quantiles,
            r#"1: {"id":1,"b":true,"i":null,"s":"1-modified"}
"#
        );
    }
}
