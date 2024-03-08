use crate::catalog::InputCollectionHandle;
use crate::{
    catalog::{NeighborhoodEntry, OutputCollectionHandles, SerCollectionHandle},
    static_compile::{DeScalarHandle, DeScalarHandleImpl},
    Catalog, ControllerError, DeserializeWithContext, SerializeWithContext,
};
use dbsp::{
    operator::{
        DelayedFeedback, MapHandle, NeighborhoodDescr, NeighborhoodDescrBox,
        NeighborhoodDescrStream, SetHandle, ZSetHandle,
    },
    typed_batch::TypedBox,
    utils::Tup2,
    DBData, OrdIndexedZSet, RootCircuit, Stream, ZSet,
};
use pipeline_types::program_schema::Relation;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

use super::{DeMapHandle, DeSetHandle, DeZSetHandle, SerCollectionHandleImpl, SqlSerdeConfig};

#[derive(Serialize, Deserialize, Debug, Default, PartialEq, Eq, Clone)]
pub struct NeighborhoodQuery<K> {
    pub anchor: Option<K>,
    pub before: u64,
    pub after: u64,
}

impl Catalog {
    fn parse_relation_schema(schema: &str) -> Result<Relation, ControllerError> {
        serde_json::from_str(schema).map_err(|e| {
            ControllerError::schema_parse_error(&format!(
                "error parsing relation schema: '{e}'. Invalid schema: '{schema}'"
            ))
        })
    }
    /// Add an input stream of Z-sets to the catalog.
    ///
    /// Adds a `DeCollectionHandle` to the catalog, which will deserialize
    /// input records into type `D` before converting them to `Z::Key` using
    /// the `From` trait.
    pub fn register_input_zset<Z, D>(
        &mut self,
        stream: Stream<RootCircuit, Z>,
        handle: ZSetHandle<Z::Key>,
        schema: &str,
    ) where
        D: for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
            + SerializeWithContext<SqlSerdeConfig>
            + From<Z::Key>
            + Clone
            + Debug
            + Send
            + 'static,
        Z: ZSet + Debug + Send + Sync,
        Z::InnerBatch: Send,
        Z::Key: Sync + From<D>,
    {
        let relation_schema: Relation = Self::parse_relation_schema(schema).unwrap();

        self.register_input_collection_handle(InputCollectionHandle::new(
            relation_schema,
            DeZSetHandle::new(handle),
        ))
        .unwrap();

        // Inputs are also outputs.
        self.register_output_zset(stream, schema);
    }

    /// Add an input stream created using `add_input_set` to catalog.
    ///
    /// Adds a `DeCollectionHandle` to the catalog, which will deserialize
    /// input records into type `D` before converting them to `Z::Key` using
    /// the `From` trait.
    pub fn register_input_set<Z, D>(
        &mut self,
        stream: Stream<RootCircuit, Z>,
        handle: SetHandle<Z::Key>,
        schema: &str,
    ) where
        D: for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
            + SerializeWithContext<SqlSerdeConfig>
            + From<Z::Key>
            + Clone
            + Debug
            + Send
            + 'static,
        Z: ZSet + Debug + Send + Sync,
        Z::InnerBatch: Send,
        Z::Key: Sync + From<D>,
    {
        let relation_schema: Relation = Self::parse_relation_schema(schema).unwrap();

        self.register_input_collection_handle(InputCollectionHandle::new(
            relation_schema,
            DeSetHandle::new(handle),
        ))
        .unwrap();

        // Inputs are also outputs.
        self.register_output_zset(stream, schema);
    }

    /// Register an input handle created using `add_input_map`.
    ///
    /// Elements are inserted by value and deleted by key.  On insert, the
    /// handle uses `key_func` to extract the key from the value.
    ///
    /// # Generics
    ///
    /// * `K` - Key type of the input collection.
    /// * `KD` - Key type in the input byte stream.  Keys will get deserialized
    ///   into instances of `KD` and then converted to `K`.
    /// * `V` - Value type of the input collection.
    /// * `VD` - Value type in the input byte stream.  Values will get
    ///   deserialized into instances of `VD` and then converted to `V`.
    /// * `U` - Update type, which specifies a modification of a record in the
    ///   collection.
    /// * `UD` - Update type in the input byte stream.  Updates will get
    ///   deserialized into instances of `UD` and then converted to `U`.
    pub fn register_input_map<K, KD, V, VD, U, UD, VF, UF>(
        &mut self,
        stream: Stream<RootCircuit, OrdIndexedZSet<K, V>>,
        handle: MapHandle<K, V, U>,
        value_key_func: VF,
        update_key_func: UF,
        schema: &str,
    ) where
        VF: Fn(&V) -> K + Clone + Send + Sync + 'static,
        UF: Fn(&U) -> K + Clone + Send + Sync + 'static,
        KD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
            + SerializeWithContext<SqlSerdeConfig>
            + From<K>
            + Send
            + 'static,
        VD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
            + SerializeWithContext<SqlSerdeConfig>
            + From<V>
            + Clone
            + Debug
            + Default
            + Send
            + 'static,
        UD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
            + SerializeWithContext<SqlSerdeConfig>
            + From<U>
            + Send
            + 'static,
        K: DBData + Sync + From<KD>,
        V: DBData + Sync + From<VD>,
        U: DBData + Sync + From<UD>,
    {
        let relation_schema: Relation = Self::parse_relation_schema(schema).unwrap();

        self.register_input_collection_handle(InputCollectionHandle::new(
            relation_schema,
            DeMapHandle::new(handle, value_key_func.clone(), update_key_func.clone()),
        ))
        .unwrap();

        // Inputs are also outputs.
        self.register_output_map(stream, value_key_func, schema);
    }

    /// Add an output stream of Z-sets to the catalog.
    pub fn register_output_zset<Z, D>(&mut self, stream: Stream<RootCircuit, Z>, schema: &str)
    where
        D: for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
            + SerializeWithContext<SqlSerdeConfig>
            + From<Z::Key>
            + Clone
            + Debug
            + Send
            + 'static,
        Z: ZSet + Debug + Send + Sync,
        Z::InnerBatch: Send,
        Z::Key: Sync + From<D>,
    {
        let schema: Relation = Self::parse_relation_schema(schema).unwrap();

        let circuit = stream.circuit();

        // Create handle for the stream itself.
        let delta_handle = stream.output();

        // Improve the odds that `integrate_trace` below reuses the trace of `stream`
        // if one exists.
        let stream = stream.try_sharded_version();

        // Create handles for the neighborhood query.
        let (neighborhood_descr_stream, neighborhood_descr_handle) =
            circuit.add_input_stream::<(bool, Option<NeighborhoodQuery<D>>)>();
        let neighborhood_stream = {
            // Create a feedback loop to latch the latest neighborhood descriptor
            // when `reset=true`.
            let feedback =
                <DelayedFeedback<RootCircuit, Option<NeighborhoodDescrBox<Z::Key, ()>>>>::new(
                    stream.circuit(),
                );
            let new_neighborhood: NeighborhoodDescrStream<Z::Key, ()> =
                feedback
                    .stream()
                    .apply2(&neighborhood_descr_stream, |old, (reset, new)| {
                        if *reset {
                            // Convert anchor of type `D` into `Z::Key`.
                            new.clone().map(|new| {
                                TypedBox::new(NeighborhoodDescr::new(
                                    new.anchor.map(From::from),
                                    (),
                                    new.before,
                                    new.after,
                                ))
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
            schema,
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

        self.register_output_batch_handles(handles).unwrap();
    }

    /// Add an output stream that carries updates to an indexed Z-set that
    /// behaves like a map (i.e., has exactly one key with weight 1 per value)
    /// to the catalog.
    ///
    /// Creates neighborhood and quantiles circuits.  Drops the key component
    /// of the `(key,value)` tuple, so that delta, neighborhood, and quantiles
    /// streams contain values only.  Clients, e.g., the web console, can
    /// work with maps and z-sets in the same way.
    pub fn register_output_map<K, KD, V, VD, F>(
        &mut self,
        stream: Stream<RootCircuit, OrdIndexedZSet<K, V>>,
        key_func: F,
        schema: &str,
    ) where
        F: Fn(&V) -> K + Clone + Send + Sync + 'static,
        KD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
            + SerializeWithContext<SqlSerdeConfig>
            + From<K>,
        VD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
            + SerializeWithContext<SqlSerdeConfig>
            + From<V>
            + Default
            + Debug
            + Clone
            + Send
            + 'static,
        K: DBData + Send + Sync + From<KD> + Default,
        V: DBData + Send + Sync + From<VD> + Default,
    {
        let circuit = stream.circuit();
        let schema: Relation = Self::parse_relation_schema(schema).unwrap();

        // Create handle for the stream itself.
        let delta_handle = stream.map(|(_k, v)| v.clone()).output();

        // Improve the odds that `integrate_trace` below reuses the trace of `stream`
        // if one exists.
        let stream = stream.try_sharded_version();

        // Create handles for the neighborhood query.
        let (neighborhood_descr_stream, neighborhood_descr_handle) =
            circuit.add_input_stream::<(bool, Option<NeighborhoodQuery<VD>>)>();
        let neighborhood_stream = {
            // Create a feedback loop to latch the latest neighborhood descriptor
            // when `reset=true`.
            let feedback = <DelayedFeedback<RootCircuit, Option<NeighborhoodDescrBox<K, V>>>>::new(
                stream.circuit(),
            );
            let new_neighborhood: NeighborhoodDescrStream<K, V> = feedback.stream().apply2(
                &neighborhood_descr_stream,
                move |old: &Option<NeighborhoodDescrBox<K, V>>,
                      (reset, new): &(bool, Option<NeighborhoodQuery<VD>>)| {
                    if *reset {
                        let key_func = key_func.clone();
                        // Convert anchor of type `(VD, ())` into `(Z::Key, Z::Val)`.
                        new.clone().map(move |new| {
                            let val = new.anchor.map(V::from);
                            let key = val.as_ref().map(key_func);
                            TypedBox::new(NeighborhoodDescr::new(
                                key,
                                val.unwrap_or_default(),
                                new.before,
                                new.after,
                            ))
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
            .map(|Tup2(idx, Tup2(_k, v))| Tup2(*idx, Tup2(v.clone(), ())))
            .output();

        // Neighborhood snapshot stream.  The integral computation
        // is essentially free thanks to stream caching.
        let neighborhood_snapshot_stream = neighborhood_stream.integrate();
        let neighborhood_snapshot_handle = neighborhood_snapshot_stream
            .map(|Tup2(idx, Tup2(_k, v))| Tup2(*idx, Tup2(v.clone(), ())))
            .output_guarded(&neighborhood_descr_stream.apply(|(reset, _descr)| *reset));

        // Handle for the quantiles query.
        let (num_quantiles_stream, num_quantiles_handle) = circuit.add_input_stream::<usize>();

        // Output of the quantiles query, only produced when `num_quantiles>0`.
        let quantiles_stream = stream
            .integrate_trace()
            .stream_unique_key_val_quantiles(&num_quantiles_stream);
        let quantiles_handle = quantiles_stream
            .map(|Tup2(_k, v)| v.clone())
            .output_guarded(&num_quantiles_stream.apply(|num_quantiles| *num_quantiles > 0));

        let handles = OutputCollectionHandles {
            schema,
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

        self.register_output_batch_handles(handles).unwrap();
    }
}

#[cfg(test)]
mod test {
    use std::{io::Write, ops::Deref};

    use crate::{
        catalog::{OutputCollectionHandles, RecordFormat},
        test::TestStruct,
        Catalog, CircuitCatalog, SerBatch,
    };
    use dbsp::Runtime;
    use pipeline_types::format::json::JsonFlavor;

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

            let (input, hinput) = circuit.add_input_map::<u32, TestStruct, TestStruct, _>(|v, u| *v = u.clone());

            catalog.register_input_map::<u32, u32, TestStruct, TestStruct, TestStruct, TestStruct, _, _>(
                input.clone(),
                hinput,
                |test_struct| test_struct.id,
                |test_struct| test_struct.id,
                r#"{"name": "input_MAP", "case_sensitive": false, "fields":[]}"#
            );

            Ok(catalog)
        })
        .unwrap();

        let input_map_handle = catalog.input_collection_handle("iNpUt_map").unwrap();
        let mut input_stream_handle = input_map_handle
            .handle
            .configure_deserializer(RECORD_FORMAT.clone())
            .unwrap();

        let output_stream_handles = catalog.output_handles("Input_map").unwrap();

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
