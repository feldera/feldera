use super::{DeMapHandle, DeSetHandle, DeZSetHandle, SerCollectionHandleImpl};
use crate::catalog::{InputCollectionHandle, SerBatchReaderHandle};
use crate::{
    catalog::{OutputCollectionHandles, SerCollectionHandle},
    Catalog, ControllerError,
};
use dbsp::typed_batch::TypedBatch;
use dbsp::{
    operator::{MapHandle, SetHandle, ZSetHandle},
    DBData, OrdIndexedZSet, RootCircuit, Stream, ZSet, ZWeight,
};
use feldera_adapterlib::catalog::CircuitCatalog;
use feldera_types::program_schema::{Relation, SqlIdentifier};
use feldera_types::serde_with_context::{
    DeserializeWithContext, SerializeWithContext, SqlSerdeConfig,
};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;

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
            + Sync
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

    /// Like `register_input_zset`, but additionally materializes the integral
    /// of the stream and makes it queryable.
    pub fn register_materialized_input_zset<Z, D>(
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
            + Sync
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
        self.register_materialized_output_zset(stream, schema);
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
            + Sync
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

    /// Like `register_input_set`, but additionally materializes the integral
    /// of the stream and makes it queryable.
    pub fn register_materialized_input_set<Z, D>(
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
            + Sync
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
        self.register_materialized_output_zset(stream, schema);
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
            + Sync
            + 'static,
        VD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
            + SerializeWithContext<SqlSerdeConfig>
            + From<V>
            + Clone
            + Debug
            + Default
            + Send
            + Sync
            + 'static,
        UD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
            + SerializeWithContext<SqlSerdeConfig>
            + From<U>
            + Send
            + Sync
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

    /// Like `register_input_map`, but additionally materializes the integral
    /// of the stream and makes it queryable.
    pub fn register_materialized_input_map<K, KD, V, VD, U, UD, VF, UF>(
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
            + Sync
            + 'static,
        VD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
            + SerializeWithContext<SqlSerdeConfig>
            + From<V>
            + Clone
            + Debug
            + Default
            + Send
            + Sync
            + 'static,
        UD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
            + SerializeWithContext<SqlSerdeConfig>
            + From<U>
            + Send
            + Sync
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
        self.register_materialized_output_map(stream, schema);
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
            + Sync
            + 'static,
        Z: ZSet + Debug + Send + Sync,
        Z::InnerBatch: Send,
        Z::Key: Sync + From<D>,
    {
        let schema: Relation = Self::parse_relation_schema(schema).unwrap();
        let name = schema.name.clone();

        // Create handle for the stream itself.
        let delta_handle = stream.output();

        let handles = OutputCollectionHandles {
            key_schema: None,
            value_schema: schema,
            index_of: None,
            delta_handle: Box::new(<SerCollectionHandleImpl<_, D, ()>>::new(delta_handle))
                as Box<dyn SerCollectionHandle>,
            integrate_handle: None,
        };

        self.register_output_batch_handles(&name, handles).unwrap();
    }

    /// Like `register_output_zset`, but additionally materializes the integral
    /// of the stream and makes it queryable.
    pub fn register_materialized_output_zset<Z, D>(
        &mut self,
        stream: Stream<RootCircuit, Z>,
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
        let schema: Relation = Self::parse_relation_schema(schema).unwrap();
        let name = schema.name.clone();

        // The integral of this stream is used by the ad hoc query engine. The engine treats the integral
        // computed by each worker as a separate partition.  This means that integrals should not contain
        // negative weights, since datafusion cannot handle those.  Negative weights can arise from operators
        // like antijoin that can produce the same record with +1 and -1 weights in different workers.
        // To avoid this, we shard the stream, so that such records get canceled out.
        let stream = stream.shard();

        // Create handle for the stream itself.
        let delta_handle = stream.output();

        let integrate_handle = stream
            .integrate_trace()
            .apply(|t| TypedBatch::<Z::Key, (), ZWeight, _>::new(t.ro_snapshot()))
            .output();

        let handles = OutputCollectionHandles {
            key_schema: None,
            value_schema: schema,
            index_of: None,
            integrate_handle: Some(Arc::new(<SerCollectionHandleImpl<_, D, ()>>::new(
                integrate_handle,
            )) as Arc<dyn SerBatchReaderHandle>),
            delta_handle: Box::new(<SerCollectionHandleImpl<_, D, ()>>::new(delta_handle))
                as Box<dyn SerCollectionHandle>,
        };

        self.register_output_batch_handles(&name, handles).unwrap();
    }

    /// Add an output stream that carries updates to an indexed Z-set that
    /// behaves like a map (i.e., has exactly one key with weight 1 per value)
    /// to the catalog.
    pub fn register_output_map<K, KD, V, VD, F>(
        &mut self,
        stream: Stream<RootCircuit, OrdIndexedZSet<K, V>>,
        _key_func: F,
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
        let schema: Relation = Self::parse_relation_schema(schema).unwrap();
        let name = schema.name.clone();

        // Create handle for the stream itself.
        let delta_handle = stream.map(|(_k, v)| v.clone()).output();

        let handles = OutputCollectionHandles {
            key_schema: None,
            value_schema: schema,
            index_of: None,
            delta_handle: Box::new(<SerCollectionHandleImpl<_, VD, ()>>::new(delta_handle))
                as Box<dyn SerCollectionHandle>,
            integrate_handle: None,
        };

        self.register_output_batch_handles(&name, handles).unwrap();
    }

    /// Like `register_output_map`, but additionally materializes the integral
    /// of the stream and makes it queryable.
    pub fn register_materialized_output_map<K, KD, V, VD>(
        &mut self,
        stream: Stream<RootCircuit, OrdIndexedZSet<K, V>>,
        schema: &str,
    ) where
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
        let schema: Relation = Self::parse_relation_schema(schema).unwrap();
        let name = schema.name.clone();

        // Create handle for the stream itself.
        let delta_handle = stream.map(|(_k, v)| v.clone()).output();

        // Improve the odds that `integrate_trace` below reuses the trace of `stream`
        // if one exists.
        let stream = stream.try_sharded_version();

        let integrate_handle = stream
            .map(|(_k, v)| v.clone())
            .integrate_trace()
            .apply(|s| TypedBatch::<V, (), ZWeight, _>::new(s.ro_snapshot()))
            .output();

        let handles = OutputCollectionHandles {
            key_schema: None,
            value_schema: schema,
            index_of: None,
            delta_handle: Box::new(<SerCollectionHandleImpl<_, VD, ()>>::new(delta_handle))
                as Box<dyn SerCollectionHandle>,
            integrate_handle: Some(Arc::new(<SerCollectionHandleImpl<_, VD, ()>>::new(
                integrate_handle,
            )) as Arc<dyn SerBatchReaderHandle>),
        };

        self.register_output_batch_handles(&name, handles).unwrap();
    }

    /// Register an index associated with output stream `view_name`.
    ///
    /// The index stream should contain the same updates as the primary
    /// stream, but as an indexed Z-set.
    pub fn register_index<K, KD, V, VD>(
        &mut self,
        stream: Stream<RootCircuit, OrdIndexedZSet<K, V>>,
        index_name: &SqlIdentifier,
        view_name: &SqlIdentifier,
        key_fields: &[&SqlIdentifier],
    ) -> Option<()>
    where
        KD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
            + SerializeWithContext<SqlSerdeConfig>
            + From<K>
            + Send
            + Debug
            + 'static,
        VD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
            + SerializeWithContext<SqlSerdeConfig>
            + From<V>
            + Send
            + Debug
            + 'static,
        K: DBData + Send + Sync + From<KD> + Default,
        V: DBData + Send + Sync + From<VD> + Default,
    {
        if self.output_handles(index_name).is_some() {
            return None;
        }

        let view_handles = self.output_handles(view_name)?;

        let stream_handle = stream.output();

        let handles = OutputCollectionHandles {
            key_schema: Some(index_schema(
                index_name,
                &view_handles.value_schema,
                key_fields,
            )),
            value_schema: view_handles.value_schema.clone(),
            index_of: Some(view_name.clone()),
            delta_handle: Box::new(<SerCollectionHandleImpl<_, KD, VD>>::new(stream_handle))
                as Box<dyn SerCollectionHandle>,
            integrate_handle: None,
        };

        self.register_output_batch_handles(index_name, handles)
            .unwrap();

        Some(())
    }
}

fn index_schema(
    index_name: &SqlIdentifier,
    base_schema: &Relation,
    key_fields: &[&SqlIdentifier],
) -> Relation {
    let mut fields = Vec::new();
    for field in key_fields.iter() {
        let base_field = base_schema
            .fields
            .iter()
            .find(|f| f.name == **field)
            .unwrap_or_else(|| panic!("column {field} not found in {}", base_schema.name))
            .clone();
        fields.push(base_field);
    }

    Relation::new(index_name.clone(), fields, false, BTreeMap::new())
}

#[cfg(test)]
mod test {
    use std::{io::Write, ops::Deref};

    use crate::{catalog::RecordFormat, test::TestStruct, Catalog, CircuitCatalog, SerBatch};
    use dbsp::Runtime;
    use feldera_types::format::json::JsonFlavor;

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

    #[test]
    fn catalog_map_handle_test() {
        let (mut circuit, catalog) = Runtime::init_circuit(4, |circuit| {
            let mut catalog = Catalog::new();

            let (input, hinput) = circuit.add_input_map::<u32, TestStruct, TestStruct, _>(|v, u| *v = u.clone());

            catalog.register_materialized_input_map::<u32, u32, TestStruct, TestStruct, TestStruct, TestStruct, _, _>(
                input.clone(),
                hinput,
                |test_struct| test_struct.id,
                |test_struct| test_struct.id,
                r#"{"name": "input_MAP", "case_sensitive": false, "fields":[]}"#
            );

            Ok(catalog)
        })
        .unwrap();

        let input_map_handle = catalog
            .input_collection_handle(&("iNpUt_map".into()))
            .unwrap();
        let mut input_stream_handle = input_map_handle
            .handle
            .configure_deserializer(RECORD_FORMAT.clone())
            .unwrap();

        let output_stream_handles = catalog.output_handles(&("Input_map".into())).unwrap();

        // Step 1: insert a couple of values.

        input_stream_handle
            .insert(br#"{"id": 1, "b": true, "s": "1"}"#)
            .unwrap();
        input_stream_handle
            .insert(br#"{"id": 2, "b": true, "s": "2"}"#)
            .unwrap();
        input_stream_handle.flush();

        circuit.step().unwrap();

        let delta = batch_to_json(output_stream_handles.delta_handle.consolidate().deref());
        assert_eq!(
            delta,
            r#"1: {"id":1,"b":true,"i":null,"s":"1"}
1: {"id":2,"b":true,"i":null,"s":"2"}
"#
        );

        // Step 2: replace an entry.

        input_stream_handle
            .insert(br#"{"id": 1, "b": true, "s": "1-modified"}"#)
            .unwrap();
        input_stream_handle.flush();

        circuit.step().unwrap();

        let delta = batch_to_json(output_stream_handles.delta_handle.consolidate().deref());
        assert_eq!(
            delta,
            r#"-1: {"id":1,"b":true,"i":null,"s":"1"}
1: {"id":1,"b":true,"i":null,"s":"1-modified"}
"#
        );

        // Step 3: delete an entry.

        input_stream_handle.delete(br#"2"#).unwrap();
        input_stream_handle.flush();

        circuit.step().unwrap();

        let delta = batch_to_json(output_stream_handles.delta_handle.consolidate().deref());
        assert_eq!(
            delta,
            r#"-1: {"id":2,"b":true,"i":null,"s":"2"}
"#
        );
    }
}
