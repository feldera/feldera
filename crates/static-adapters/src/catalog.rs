//! The catalog of a circuit that the SQL compiler generates: its input and output streams, by
//! name, which the generated code registers through the record types of each stream.

use crate::{DeLazyMapHandle, DeMapHandle, DeZSetHandle, SerCollectionHandleImpl};
use dbsp::circuit::Layout;
use dbsp::circuit::circuit_builder::CircuitBase;
use dbsp::dynamic::DynData;
use dbsp::operator::dynamic::accumulator::EnableCount;
use dbsp::trace::spine_async::WithSnapshot;
use dbsp::typed_batch::{Spine, TypedBatch};
use dbsp::utils::Tup1;
use dbsp::{Batch, Circuit as _, OrdZSet, Runtime};
use dbsp::{
    DBData, OrdIndexedZSet, RootCircuit, Stream, ZSet, ZWeight,
    operator::{LazyMapHandle, MapHandle, ZSetHandle},
    typed_batch::BatchReader,
};
use feldera_adapterlib::{
    errors::controller::ControllerError, postprocess::PostprocessorRegistry,
    preprocess::PreprocessorRegistry,
};
use feldera_sqllib::{SqlString, SqlVariant, build_string_interner};
use feldera_types::program_schema::{Relation, SqlIdentifier};
use feldera_types::serde_with_context::{
    DeserializeWithContext, SerializeWithContext, SqlSerdeConfig,
};
use std::any::TypeId;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::mem::transmute;
use std::sync::{Arc, Mutex};

pub use feldera_adapterlib::catalog::*;

/// Circuit catalog implementation.
pub struct Catalog {
    input_collection_handles: BTreeMap<SqlIdentifier, InputCollectionHandle>,
    output_batch_handles: BTreeMap<SqlIdentifier, OutputCollectionHandles>,
    preprocessor_registry: Arc<Mutex<PreprocessorRegistry>>,
    postprocessor_registry: Arc<Mutex<PostprocessorRegistry>>,
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            input_collection_handles: BTreeMap::new(),
            output_batch_handles: BTreeMap::new(),
            preprocessor_registry: Arc::new(Mutex::new(PreprocessorRegistry::new())),
            postprocessor_registry: Arc::new(Mutex::new(PostprocessorRegistry::new())),
        }
    }

    pub fn register_input_collection_handle(
        &mut self,
        handle: InputCollectionHandle,
    ) -> Result<(), ControllerError> {
        let name = &handle.schema.name;
        if self.input_collection_handles.contains_key(name) {
            return Err(ControllerError::duplicate_input_stream(&name.sql_name()));
        }
        self.input_collection_handles.insert(name.clone(), handle);

        Ok(())
    }

    pub fn register_output_batch_handles(
        &mut self,
        name: &SqlIdentifier,
        handles: OutputCollectionHandles,
    ) -> Result<(), ControllerError> {
        if self.output_batch_handles.contains_key(name) {
            return Err(ControllerError::duplicate_output_stream(&name.sql_name()));
        }
        self.output_batch_handles.insert(name.clone(), handles);

        Ok(())
    }
}

impl CircuitCatalog for Catalog {
    /// Look up an input stream handle by name.
    fn input_collection_handle(&self, name: &SqlIdentifier) -> Option<&InputCollectionHandle> {
        self.input_collection_handles.get(name)
    }

    fn input_iter(
        &self,
    ) -> Box<dyn Iterator<Item = (&SqlIdentifier, &InputCollectionHandle)> + '_> {
        Box::new(self.input_collection_handles.iter())
    }

    /// Look up output stream handles by name.
    fn output_handles(&self, name: &SqlIdentifier) -> Option<&OutputCollectionHandles> {
        self.output_batch_handles.get(name)
    }

    fn index_handles(
        &self,
        endpoint_name: &str,
        stream: &SqlIdentifier,
        index: &SqlIdentifier,
    ) -> Result<&OutputCollectionHandles, ControllerError> {
        let Some(stream_handles) = self.output_handles(stream) else {
            return Err(ControllerError::unknown_output_stream(
                endpoint_name,
                &stream.sql_name(),
            ));
        };

        // The index is the same as the stream.
        if stream_handles.alias_as_index == Some(index.clone()) {
            return Ok(stream_handles);
        }

        let handle = self
            .output_handles(index)
            .ok_or_else(|| ControllerError::unknown_index(endpoint_name, &index.sql_name()))?;

        if handle.index_of.is_none() {
            return Err(ControllerError::not_an_index(
                endpoint_name,
                &index.sql_name(),
            ));
        }

        if handle.index_of.as_ref() != Some(stream) {
            return Err(ControllerError::unknown_output_stream(
                endpoint_name,
                &stream.sql_name(),
            ));
        }

        Ok(handle)
    }

    fn output_handles_mut(&mut self, name: &SqlIdentifier) -> Option<&mut OutputCollectionHandles> {
        self.output_batch_handles.get_mut(name)
    }

    fn output_iter(
        &self,
    ) -> Box<dyn Iterator<Item = (&SqlIdentifier, &OutputCollectionHandles)> + '_> {
        Box::new(self.output_batch_handles.iter())
    }

    fn preprocessor_registry(&self) -> Arc<Mutex<PreprocessorRegistry>> {
        self.preprocessor_registry.clone()
    }

    fn postprocessor_registry(&self) -> Arc<Mutex<PostprocessorRegistry>> {
        self.postprocessor_registry.clone()
    }
}

const INTERNED_STRING_RELATION_NAME: &str = "feldera_interned_strings";

pub type OutputMapping = BTreeMap<String, usize>;

/// Global mapping from the name of a stream to the ordinal of the host for its
/// output connectors.
///
/// It would be better if we could pass this in to the circuit instead of having
/// a global.
pub static OUTPUT_MAPPING: Mutex<OutputMapping> = Mutex::new(BTreeMap::new());

impl Catalog {
    fn parse_relation_schema(schema: &str) -> Result<Relation, ControllerError> {
        serde_json_path_to_error::from_str(schema).map_err(|e| {
            ControllerError::schema_parse_error(&format!(
                "error parsing relation schema: '{e}'. Invalid schema: '{schema}'"
            ))
        })
    }

    /// Generate persistent id for the output operator for stream `stream`.
    /// Returns `None` if the stream does not have a persistent id.
    fn output_persistent_id<T>(stream: &Stream<RootCircuit, T>) -> Option<String> {
        stream
            .get_persistent_id()
            .map(|pid| format!("{pid}.output"))
    }

    /// Gather output stream (table or view) to the host that it is assigned to.
    /// Returns the accumulated stream and, optionally, its integral.
    ///
    /// # Arguments
    ///
    /// * `shard` - whether to shard the stream in a single-host configuration, where no cross-node
    ///   echange is needed. In the multihost configuration the output stream will be
    ///   unconditionally sharded across all workers on the target host.
    /// * `integrate` - whether to create an integral of the gathered stream.
    ///
    /// # Returns
    ///
    /// * Accumulated gathered stream
    /// * The number of active consumers of the stream, which can be used with OutputCollectionHandles to
    ///   enable the accumulator at runtime
    /// * If `integrate` is `true`, the integral of the accumulated stream.
    #[allow(clippy::type_complexity)]
    fn gather_output_to_host<Z>(
        &self,
        name: &SqlIdentifier,
        stream: &Stream<RootCircuit, Z>,
        shard: bool,
        integrate: bool,
    ) -> (
        Stream<RootCircuit, Option<Spine<Z>>>,
        EnableCount,
        Option<Stream<RootCircuit, Spine<Z>>>,
    )
    where
        Z: Batch<Time = ()>,
        Z::InnerBatch: Send,
    {
        if let Some(runtime) = Runtime::runtime()
            && let layout = runtime.layout()
            && let Layout::Multihost { hosts, .. } = layout
        {
            let ordinal = OUTPUT_MAPPING
                .lock()
                .unwrap()
                .get(&name.name())
                .copied()
                .unwrap_or_default();

            let (accumulated_stream, enabled_count) = stream
                .shard_workers_accumulate(hosts[ordinal].workers.clone())
                .into_parts();

            let integral = if integrate {
                Some(
                    stream.shard_workers_accumulate_integrate_trace(hosts[ordinal].workers.clone()),
                )
            } else {
                None
            };

            (accumulated_stream, enabled_count, integral)
        } else if shard {
            let (accumulated_stream, enabled_count) = stream.shard_accumulate().into_parts();

            let integral = if integrate {
                // Avoid bootstrapping when upgrading from an older Feldera version using shard().accumulate().
                Some(stream.shard_accumulate_integrate_trace_legacy())
            } else {
                None
            };

            (accumulated_stream, enabled_count, integral)
        } else {
            let (accumulated_stream, enabled_count) = stream.accumulate().into_parts();
            let integral = if integrate {
                Some(stream.accumulate_integrate_trace())
            } else {
                None
            };

            (accumulated_stream, enabled_count, integral)
        }
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
        D: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<Z::Key>
            + Clone
            + Debug
            + Send
            + Sync
            + 'static,
        Z: ZSet<DynK = DynData> + Debug + Send + Sync,
        Z::InnerBatch: Send,
        Z::Key: Sync + From<D>,
    {
        let relation_schema: Relation = Self::parse_relation_schema(schema).unwrap();

        self.register_input_collection_handle(InputCollectionHandle::new(
            relation_schema.clone(),
            DeZSetHandle::new(handle),
            stream.get_persistent_id(),
        ))
        .unwrap();

        let circuit = stream.circuit().clone();
        circuit.region(
            &format!("create table {}", relation_schema.name.name()),
            move || {
                // Inputs are also outputs.
                self.register_output_zset_persistent_inner(
                    Self::output_persistent_id(&stream).as_deref(),
                    stream,
                    &relation_schema,
                );
            },
        );
    }

    /// Like `register_input_zset`, but additionally materializes the integral
    /// of the stream and makes it queryable.
    pub fn register_materialized_input_zset<Z, D>(
        &mut self,
        stream: Stream<RootCircuit, Z>,
        handle: ZSetHandle<Z::Key>,
        schema: &str,
    ) where
        D: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<Z::Key>
            + Clone
            + Debug
            + Send
            + Sync
            + 'static,
        Z: ZSet<DynK = DynData> + Debug + Send + Sync,
        Z::InnerBatch: Send,
        Z::Key: Sync + From<D>,
    {
        let relation_schema: Relation = Self::parse_relation_schema(schema).unwrap();

        self.register_input_collection_handle(InputCollectionHandle::new(
            relation_schema.clone(),
            DeZSetHandle::new(handle),
            stream.get_persistent_id(),
        ))
        .unwrap();

        let circuit = stream.circuit().clone();
        circuit.region(
            &format!("create materialized table {}", relation_schema.name.name()),
            move || {
                // Inputs are also outputs.
                self.register_materialized_output_zset_persistent_inner(
                    Self::output_persistent_id(&stream).as_deref(),
                    stream,
                    &relation_schema,
                    false,
                );
            },
        );
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
        KD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<K>
            + Send
            + Sync
            + Debug
            + 'static,
        VD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<V>
            + Clone
            + Debug
            + Default
            + Send
            + Sync
            + 'static,
        UD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
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

        let Some(primary_key) = relation_schema.primary_key.clone() else {
            panic!(
                "Primary key not found for relation {}",
                relation_schema.name
            );
        };

        self.register_input_collection_handle(InputCollectionHandle::new(
            relation_schema.clone(),
            DeMapHandle::new(handle, value_key_func.clone(), update_key_func.clone()),
            stream.get_persistent_id(),
        ))
        .unwrap();

        let key_schema_name =
            SqlIdentifier::new(format!("{}.key", relation_schema.name.name()), false);

        let circuit = stream.circuit().clone();
        circuit.region(
            &format!("create table {}", relation_schema.name.name()),
            move || {
                // Inputs are also outputs.
                let handles = self
                    .register_output_map_persistent_inner(
                        Self::output_persistent_id(&stream).as_deref(),
                        stream,
                        None,
                        None,
                        &relation_schema,
                        &key_schema_name,
                        true,
                        false,
                        false,
                        primary_key.as_slice(),
                    )
                    .unwrap();

                self.register_output_batch_handles(&relation_schema.name, handles)
                    .unwrap();
            },
        );
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
        KD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<K>
            + Send
            + Sync
            + Debug
            + 'static,
        VD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<V>
            + Clone
            + Debug
            + Default
            + Send
            + Sync
            + 'static,
        UD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
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

        let Some(primary_key) = relation_schema.primary_key.clone() else {
            panic!(
                "Primary key not found for relation {}",
                relation_schema.name
            );
        };

        self.register_input_collection_handle(InputCollectionHandle::new(
            relation_schema.clone(),
            DeMapHandle::new(handle, value_key_func.clone(), update_key_func.clone()),
            stream.get_persistent_id(),
        ))
        .unwrap();

        let key_schema_name =
            SqlIdentifier::new(format!("{}.key", relation_schema.name.name()), false);

        let circuit = stream.circuit().clone();
        circuit.region(
            &format!("create materialized table {}", relation_schema.name.name()),
            move || {
                // Inputs are also outputs.
                let handles = self
                    .register_output_map_persistent_inner(
                        Self::output_persistent_id(&stream).as_deref(),
                        stream,
                        None,
                        None,
                        &relation_schema,
                        &key_schema_name,
                        true,
                        true,
                        false,
                        primary_key.as_slice(),
                    )
                    .unwrap();

                self.register_output_batch_handles(&relation_schema.name, handles)
                    .unwrap();
            },
        );
    }

    /// Like `register_materialized_input_map`, for a table fed through a lazy
    /// input map ([`RootCircuit::add_lazy_input_map`]).
    ///
    /// The map keeps its own integral built via an accumulator.
    ///
    /// # Arguments
    ///
    /// * `stream` - the table's delta stream, which downstream views read.
    /// * `handle` - the lazy input map's write handle, which the adapters feed.
    /// * `value_key_func` - extracts a record's primary key, which is how a delete
    ///   names the record it removes.
    /// * `schema` - the table's schema, as the SQL compiler emits it.
    pub fn register_lazy_materialized_input_map<K, KD, V, VD, VF>(
        &mut self,
        stream: Stream<RootCircuit, OrdIndexedZSet<K, V>>,
        handle: LazyMapHandle<K, V>,
        value_key_func: VF,
        schema: &str,
    ) where
        VF: Fn(&V) -> K + Clone + Send + Sync + 'static,
        KD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<K>
            + Send
            + Sync
            + Debug
            + 'static,
        VD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<V>
            + Clone
            + Debug
            + Default
            + Send
            + Sync
            + 'static,
        K: DBData + Sync + From<KD>,
        V: DBData + Sync + From<VD>,
    {
        let relation_schema: Relation = Self::parse_relation_schema(schema).unwrap();
        let Some(primary_key) = relation_schema.primary_key.clone() else {
            panic!(
                "Primary key not found for relation {}",
                relation_schema.name
            );
        };

        self.register_input_collection_handle(InputCollectionHandle::new(
            relation_schema.clone(),
            DeLazyMapHandle::new(handle, value_key_func.clone()),
            stream.get_persistent_id(),
        ))
        .unwrap();

        let key_schema_name =
            SqlIdentifier::new(format!("{}.key", relation_schema.name.name()), false);

        let circuit = stream.circuit().clone();
        circuit.region(
            &format!("create materialized table {}", relation_schema.name.name()),
            move || {
                // Inputs are also outputs.
                let handles = self
                    .register_output_map_persistent_inner(
                        Self::output_persistent_id(&stream).as_deref(),
                        stream,
                        None,
                        None,
                        &relation_schema,
                        &key_schema_name,
                        true,
                        true,
                        true,
                        primary_key.as_slice(),
                    )
                    .unwrap();
                self.register_output_batch_handles(&relation_schema.name, handles)
                    .unwrap();
            },
        );
    }

    /// Add an output stream of Z-sets to the catalog.
    pub fn register_output_zset<Z, D>(&mut self, stream: Stream<RootCircuit, Z>, schema: &str)
    where
        D: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<Z::Key>
            + Clone
            + Debug
            + Send
            + Sync
            + 'static,
        Z: ZSet<DynK = DynData> + Debug + Send + Sync,
        Z::InnerBatch: Send,
        Z::Key: Sync + From<D>,
    {
        self.register_output_zset_persistent(None, stream, schema)
    }

    /// Add an output stream of Z-sets to the catalog, assigning a persistent id
    /// to the output operator.
    ///
    /// Output streams with new persistent ids will be bootstrapped when resuming
    /// the pipeline from a checkpoint by computing the entire contents of the stream
    /// from upstream operators and sending it to the output handle.
    pub fn register_output_zset_persistent<Z, D>(
        &mut self,
        persistent_id: Option<&str>,
        stream: Stream<RootCircuit, Z>,
        schema: &str,
    ) where
        D: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<Z::Key>
            + Clone
            + Debug
            + Send
            + Sync
            + 'static,
        Z: ZSet<DynK = DynData> + Debug + Send + Sync,
        Z::InnerBatch: Send,
        Z::Key: Sync + From<D>,
    {
        let schema: Relation = Self::parse_relation_schema(schema).unwrap();
        let name = schema.name.clone();

        let circuit = stream.circuit().clone();

        circuit.region(&format!("create view {}", name.name()), move || {
            self.register_output_zset_persistent_inner(persistent_id, stream, &schema)
        });
    }

    pub fn register_output_zset_persistent_inner<Z, D>(
        &mut self,
        persistent_id: Option<&str>,
        stream: Stream<RootCircuit, Z>,
        schema: &Relation,
    ) where
        D: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<Z::Key>
            + Clone
            + Debug
            + Send
            + Sync
            + 'static,
        Z: ZSet<DynK = DynData> + Debug + Send + Sync,
        Z::InnerBatch: Send,
        Z::Key: Sync + From<D>,
    {
        let name = schema.name.clone();
        let circuit = stream.circuit();

        if name == SqlIdentifier::new(INTERNED_STRING_RELATION_NAME, false) {
            if TypeId::of::<Z>() != TypeId::of::<OrdZSet<Tup1<SqlString>>>() {
                panic!(
                    "Reserved relation {INTERNED_STRING_RELATION_NAME} must have type OrdZSet<Tup1<SqlString>>, but it was declared with type {}",
                    std::any::type_name::<Z>()
                );
            } else {
                let stream = unsafe {
                    transmute::<Stream<RootCircuit, Z>, Stream<RootCircuit, OrdZSet<Tup1<SqlString>>>>(
                        stream.clone(),
                    )
                };
                build_string_interner(stream, None)
            }
        }

        let (stream, enable_count, _) = self.gather_output_to_host(&name, &stream, false, false);

        // Create handle for the stream itself.
        let (delta_handle, delta_gid) = circuit.output_accumulated_stream_persistent_with_gid::<Z>(
            &stream,
            enable_count.clone(),
            persistent_id,
        );
        circuit.set_mir_node_id(&delta_gid, persistent_id);

        let handles = OutputCollectionHandles {
            key_schema: None,
            value_schema: schema.clone(),
            index_of: None,
            alias_as_index: None,
            delta_handle: Box::new(<SerCollectionHandleImpl<_, D, ()>>::new(delta_handle))
                as Box<dyn SerBatchReaderHandle>,
            enable_count,
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
        D: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<Z::Key>
            + Clone
            + Debug
            + Send
            + 'static,
        Z: ZSet<DynK = DynData> + Debug + Send + Sync,
        Z::InnerBatch: Send,
        Z::Key: Sync + From<D>,
    {
        self.register_materialized_output_zset_persistent(None, stream, schema)
    }

    /// Like `register_output_zset`, but additionally materializes the integral
    /// of the stream and makes it queryable.
    pub fn register_materialized_output_zset_persistent<Z, D>(
        &mut self,
        persistent_id: Option<&str>,
        stream: Stream<RootCircuit, Z>,
        schema: &str,
    ) where
        D: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<Z::Key>
            + Clone
            + Debug
            + Send
            + 'static,
        Z: ZSet<DynK = DynData> + Debug + Send + Sync,
        Z::InnerBatch: Send,
        Z::Key: Sync + From<D>,
    {
        let circuit = stream.circuit().clone();
        let schema: Relation = Self::parse_relation_schema(schema).unwrap();
        let name = schema.name.clone();

        circuit.region(
            &format!("create materialized view {}", name.name()),
            move || {
                self.register_materialized_output_zset_persistent_inner(
                    persistent_id,
                    stream,
                    &schema,
                    true,
                )
            },
        );
    }

    /// `gather_integral` indicates whether the stream must be materialized on
    /// the host assigned to this stream. This is necessary in order to be able
    /// to send a snapshot of the collection to a connector attached to this stream.
    pub fn register_materialized_output_zset_persistent_inner<Z, D>(
        &mut self,
        persistent_id: Option<&str>,
        stream: Stream<RootCircuit, Z>,
        schema: &Relation,
        gather_integral: bool,
    ) where
        D: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<Z::Key>
            + Clone
            + Debug
            + Send
            + 'static,
        Z: ZSet<DynK = DynData> + Debug + Send + Sync,
        Z::InnerBatch: Send,
        Z::Key: Sync + From<D>,
    {
        let name = schema.name.clone();
        let circuit = stream.circuit();

        // The integral of this stream is used by the ad hoc query
        // engine. The engine treats the integral computed by each
        // worker as a separate partition.  This means that
        // integrals should not contain negative weights, since
        // datafusion cannot handle those.  Negative weights can
        // arise from operators like antijoin that can produce the
        // same record with +1 and -1 weights in different workers.
        // To avoid this, we shard the stream, so that such records
        // get canceled out.
        let (gathered_stream, enable_count, gathered_integral_stream) =
            self.gather_output_to_host(&name, &stream, true, gather_integral);

        // Create handle for the stream itself.
        let (delta_handle, delta_gid) = circuit.output_accumulated_stream_persistent_with_gid::<Z>(
            &gathered_stream,
            enable_count.clone(),
            persistent_id,
        );
        circuit.set_mir_node_id(&delta_gid, persistent_id);

        let integral_stream = if let Some(gathered_integral_stream) = gathered_integral_stream {
            gathered_integral_stream
        } else {
            stream.accumulate_integrate_trace()
        };

        let (integrate_handle, integrate_gid) = integral_stream
            .apply(|t| TypedBatch::<Z::Key, (), ZWeight, _>::new(t.inner().ro_snapshot()))
            .output_persistent_with_gid(
                persistent_id.map(|id| format!("{id}.integral")).as_deref(),
            );

        circuit.set_mir_node_id(&integrate_gid, persistent_id);

        let handles = OutputCollectionHandles {
            key_schema: None,
            value_schema: schema.clone(),
            index_of: None,
            alias_as_index: None,
            integrate_handle: Some(Arc::new(<SerCollectionHandleImpl<_, D, ()>>::new(
                integrate_handle,
            )) as Arc<dyn SerBatchReaderHandle>),
            delta_handle: Box::new(<SerCollectionHandleImpl<_, D, ()>>::new(delta_handle))
                as Box<dyn SerBatchReaderHandle>,
            enable_count,
        };

        self.register_output_batch_handles(&name, handles).unwrap();
    }

    /// Register a materialized view backed by an indexed Z-set.
    ///
    /// The same stream can double as an index over this view. If so,
    /// `alias_as_index` should be set to the index name.
    pub fn register_materialized_output_map<K, KD, V, VD>(
        &mut self,
        stream: Stream<RootCircuit, OrdIndexedZSet<K, V>>,
        alias_as_index: Option<SqlIdentifier>,
        schema: &str,
        key_fields: &[String],
    ) where
        KD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<K>
            + Send
            + Sync
            + Debug
            + 'static,
        VD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
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
        self.register_materialized_output_map_persistent(
            None,
            stream,
            alias_as_index,
            schema,
            key_fields,
        )
    }

    pub fn register_materialized_output_map_persistent<K, KD, V, VD>(
        &mut self,
        persistent_id: Option<&str>,
        stream: Stream<RootCircuit, OrdIndexedZSet<K, V>>,
        alias_as_index: Option<SqlIdentifier>,
        schema: &str,
        key_fields: &[String],
    ) where
        KD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<K>
            + Send
            + Sync
            + Debug
            + 'static,
        VD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
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
        let circuit = stream.circuit().clone();
        let schema: Relation = Self::parse_relation_schema(schema).unwrap();

        circuit.region(
            &format!("create materialized view {}", schema.name.name()),
            move || {
                let handles = self
                    .register_output_map_persistent_inner(
                        persistent_id,
                        stream,
                        None,
                        alias_as_index,
                        &schema,
                        &SqlIdentifier::new(format!("{}.key", schema.name.name()), false),
                        false,
                        true,
                        true,
                        key_fields,
                    )
                    .unwrap();

                self.register_output_batch_handles(&schema.name, handles)
                    .unwrap();
            },
        );
    }

    /// Register an output indexed Z-set stream with unique value per key.
    ///
    /// # Arguments
    ///
    /// * `index_of` - This stream is an index of the stream `index_of`.
    /// * `alias_as_index` - The same stream doubles as an index.
    /// * `schema` - Value schema.
    /// * `key_schema_name` - The name of the key schema.
    /// * `materialized` - Whether to materialize the output.
    /// * `accumulate` - if `materialized` is true, determines whether `accumulate_integrate_trace`
    ///   or `integrate_trace` is used to compute the integral.
    /// * `key_fields` - The subset of value fields to include in the key schema.
    #[allow(clippy::too_many_arguments)]
    pub fn register_output_map_persistent_inner<K, KD, V, VD>(
        &mut self,
        persistent_id: Option<&str>,
        stream: Stream<RootCircuit, OrdIndexedZSet<K, V>>,
        index_of: Option<SqlIdentifier>,
        alias_as_index: Option<SqlIdentifier>,
        schema: &Relation,
        key_schema_name: &SqlIdentifier,
        is_input: bool,
        materialized: bool,
        accumulate: bool,
        key_fields: &[String],
    ) -> Option<OutputCollectionHandles>
    where
        KD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<K>
            + Send
            + Sync
            + Debug
            + 'static,
        VD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
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
        let name = schema.name.clone();
        let circuit = stream.circuit();

        let stream = stream.try_sharded_version();

        // Shard the stream if it needs to be materialized.
        let (gathered_stream, enable_count, gathered_integral_stream) = self.gather_output_to_host(
            &name,
            &stream,
            materialized && accumulate,
            materialized && !is_input,
        );

        let (delta_handle, delta_gid) = circuit
            .output_accumulated_stream_persistent_with_gid::<OrdIndexedZSet<K, V>>(
                &gathered_stream,
                enable_count.clone(),
                persistent_id,
            );
        circuit.set_mir_node_id(&delta_gid, persistent_id);

        let integrate_handle = if materialized {
            let (integrate_handle, integral_gid) =
                if let Some(gathered_integral_stream) = gathered_integral_stream {
                    gathered_integral_stream
                } else if accumulate {
                    stream.shard_accumulate_integrate_trace()
                } else {
                    // This is an integral of an input table with a primary key. We don't support sending a snapshot
                    // of table to a connector, so we don't need to use the gathered stream.
                    // `integrate_trace` should return the existing integral created by the InputUpsert operator.
                    stream.shard().integrate_trace()
                }
                .apply(|s| TypedBatch::<K, V, ZWeight, _>::new(s.inner().ro_snapshot()))
                .output_persistent_with_gid(
                    persistent_id
                        .map(|id| format!("{id}.output_integral"))
                        .as_deref(),
                );

            circuit.set_mir_node_id(&integral_gid, persistent_id);
            Some(
                Arc::new(<SerCollectionHandleImpl<_, KD, VD>>::new(integrate_handle))
                    as Arc<dyn SerBatchReaderHandle>,
            )
        } else {
            None
        };

        Some(OutputCollectionHandles {
            key_schema: Some(index_schema(key_schema_name, schema, key_fields)),
            value_schema: schema.clone(),
            index_of,
            alias_as_index,
            delta_handle: Box::new(<SerCollectionHandleImpl<_, KD, VD>>::new(delta_handle))
                as Box<dyn SerBatchReaderHandle>,
            enable_count,
            integrate_handle,
        })
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
        key_fields: &[String],
    ) -> Option<()>
    where
        KD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<K>
            + Send
            + Sync
            + Debug
            + 'static,
        VD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<V>
            + Default
            + Clone
            + Sync
            + Send
            + Debug
            + 'static,
        K: DBData + Send + Sync + From<KD> + Default,
        V: DBData + Send + Sync + From<VD> + Default,
    {
        self.register_index_persistent(None, stream, index_name, view_name, key_fields)
    }

    /// Like `register_index`, but also assigns persistent id to the index.
    pub fn register_index_persistent<K, KD, V, VD>(
        &mut self,
        persistent_id: Option<&str>,
        stream: Stream<RootCircuit, OrdIndexedZSet<K, V>>,
        index_name: &SqlIdentifier,
        view_name: &SqlIdentifier,
        key_fields: &[String],
    ) -> Option<()>
    where
        KD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<K>
            + Send
            + Sync
            + Debug
            + 'static,
        VD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<V>
            + Default
            + Clone
            + Send
            + Sync
            + Debug
            + 'static,
        K: DBData + Send + Sync + From<KD> + Default,
        V: DBData + Send + Sync + From<VD> + Default,
    {
        if self.output_handles(index_name).is_some() {
            return None;
        }
        let value_schema = self.output_handles(view_name)?.value_schema.clone();

        let circuit = stream.circuit().clone();
        circuit.region(&format!("create index {}", index_name.name()), move || {
            let handles = self.register_output_map_persistent_inner(
                persistent_id,
                stream,
                Some(view_name.clone()),
                None,
                &value_schema,
                index_name,
                false,
                false,
                false,
                key_fields,
            )?;

            self.register_output_batch_handles(index_name, handles)
                .unwrap();
            Some(())
        })?;

        Some(())
    }

    /// Register a materialized index associated with output stream `view_name`.
    ///
    /// The index stream should contain the same updates as the primary
    /// stream, but as an indexed Z-set.
    pub fn register_materialized_index<K, KD, V, VD>(
        &mut self,
        stream: Stream<RootCircuit, OrdIndexedZSet<K, V>>,
        index_name: &SqlIdentifier,
        view_name: &SqlIdentifier,
        key_fields: &[String],
    ) -> Option<()>
    where
        KD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<K>
            + Send
            + Sync
            + Debug
            + 'static,
        VD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<V>
            + Default
            + Clone
            + Sync
            + Send
            + Debug
            + 'static,
        K: DBData + Send + Sync + From<KD> + Default,
        V: DBData + Send + Sync + From<VD> + Default,
    {
        self.register_materialized_index_persistent(None, stream, index_name, view_name, key_fields)
    }

    /// Like `register_materialized_index`, but also assigns persistent id to the index.
    pub fn register_materialized_index_persistent<K, KD, V, VD>(
        &mut self,
        persistent_id: Option<&str>,
        stream: Stream<RootCircuit, OrdIndexedZSet<K, V>>,
        index_name: &SqlIdentifier,
        view_name: &SqlIdentifier,
        key_fields: &[String],
    ) -> Option<()>
    where
        KD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<K>
            + Send
            + Sync
            + Debug
            + 'static,
        VD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<V>
            + Default
            + Clone
            + Send
            + Sync
            + Debug
            + 'static,
        K: DBData + Send + Sync + From<KD> + Default,
        V: DBData + Send + Sync + From<VD> + Default,
    {
        if self.output_handles(index_name).is_some() {
            return None;
        }
        let value_schema = self.output_handles(view_name)?.value_schema.clone();

        let circuit = stream.circuit().clone();
        circuit.region(
            &format!("create materialized index {}", index_name.name()),
            move || {
                let handles = self.register_output_map_persistent_inner(
                    persistent_id,
                    stream,
                    Some(view_name.clone()),
                    None,
                    &value_schema,
                    index_name,
                    false,
                    true,
                    true,
                    key_fields,
                )?;

                self.register_output_batch_handles(index_name, handles)
                    .unwrap();
                Some(())
            },
        )?;

        Some(())
    }
}

fn index_schema(
    index_name: &SqlIdentifier,
    base_schema: &Relation,
    key_fields: &[String],
) -> Relation {
    let mut fields = Vec::new();
    for field in key_fields.iter() {
        let base_field = base_schema
            .fields
            .iter()
            .find(|f| f.name.name() == *field)
            .unwrap_or_else(|| panic!("column {field} not found in {}", base_schema.name))
            .clone();
        fields.push(base_field);
    }

    Relation::new(index_name.clone(), fields, false, BTreeMap::new())
}

#[cfg(test)]
mod test {
    use std::{io::Write, ops::Deref, sync::Arc};

    use crate::{Catalog, CircuitCatalog, catalog::RecordFormat, test::TestStruct};
    use dbsp::Runtime;
    use feldera_adapterlib::catalog::SerBatchReader;
    use feldera_types::format::json::JsonFlavor;

    const RECORD_FORMAT: RecordFormat = RecordFormat::Json(JsonFlavor::Default);

    fn batch_to_json(batch: &dyn SerBatchReader) -> String {
        let mut cursor = batch.cursor(RECORD_FORMAT.clone()).unwrap();
        let mut result = Vec::new();

        while cursor.key_valid() {
            while cursor.val_valid() {
                write!(&mut result, "{}: ", cursor.weight()).unwrap();
                cursor.serialize_val(&mut result).unwrap();
                result.push(b'\n');
                cursor.step_val();
            }
            cursor.step_key().unwrap();
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
                r#"{
                "name": "input_MAP",
                "case_sensitive": false,
                "fields": [
                    {"name":"id","case_sensitive":false,"columntype":{"nullable":false,"type":"BIGINT"},"unused":false},
                    {"name":"b","case_sensitive":false,"columntype":{"nullable":true,"type":"BOOLEAN"},"unused":false},
                    {"name":"i","case_sensitive":false,"columntype":{"nullable":true,"type":"BIGINT"},"unused":false},
                    {"name":"s","case_sensitive":false,"columntype":{"nullable":true,"precision":-1,"type":"VARCHAR"},"unused":false}],
                "primary_key": ["id"]}"#
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
        output_stream_handles.enable_count.enable();

        // Step 1: insert a couple of values.

        input_stream_handle
            .insert(br#"{"id": 1, "b": true, "s": "1"}"#, &None)
            .unwrap();
        input_stream_handle
            .insert(br#"{"id": 2, "b": true, "s": "2"}"#, &None)
            .unwrap();
        input_stream_handle.flush().unwrap();

        circuit.transaction().unwrap();

        let delta = batch_to_json(
            output_stream_handles
                .delta_handle
                .take_readers()
                .whole
                .deref(),
        );
        assert_eq!(
            delta,
            r#"1: {"id":1,"b":true,"i":null,"s":"1"}
1: {"id":2,"b":true,"i":null,"s":"2"}
"#
        );

        // Step 2: replace an entry.

        input_stream_handle
            .insert(br#"{"id": 1, "b": true, "s": "1-modified"}"#, &None)
            .unwrap();
        input_stream_handle.flush().unwrap();

        circuit.transaction().unwrap();

        let delta = batch_to_json(
            output_stream_handles
                .delta_handle
                .take_readers()
                .whole
                .deref(),
        );
        assert_eq!(
            delta,
            r#"-1: {"id":1,"b":true,"i":null,"s":"1"}
1: {"id":1,"b":true,"i":null,"s":"1-modified"}
"#
        );

        // Step 3: delete an entry.

        input_stream_handle.delete(br#"2"#, &None).unwrap();
        input_stream_handle.flush().unwrap();

        circuit.transaction().unwrap();

        let delta = batch_to_json(
            output_stream_handles
                .delta_handle
                .take_readers()
                .whole
                .deref(),
        );
        assert_eq!(
            delta,
            r#"-1: {"id":2,"b":true,"i":null,"s":"2"}
"#
        );
    }

    /// One take reads a step's output whole and in parts, and leaves nothing for the next.
    #[test]
    fn take_readers_reads_a_step_whole_and_in_parts() {
        let (mut circuit, catalog) = Runtime::init_circuit(4, |circuit| {
            let mut catalog = Catalog::new();

            let (input, hinput) = circuit.add_input_map::<u32, TestStruct, TestStruct, _>(|v, u| *v = u.clone());

            catalog.register_materialized_input_map::<u32, u32, TestStruct, TestStruct, TestStruct, TestStruct, _, _>(
                input.clone(),
                hinput,
                |test_struct| test_struct.id,
                |test_struct| test_struct.id,
                r#"{
                "name": "input_MAP",
                "case_sensitive": false,
                "fields": [
                    {"name":"id","case_sensitive":false,"columntype":{"nullable":false,"type":"BIGINT"},"unused":false},
                    {"name":"b","case_sensitive":false,"columntype":{"nullable":true,"type":"BOOLEAN"},"unused":false},
                    {"name":"i","case_sensitive":false,"columntype":{"nullable":true,"type":"BIGINT"},"unused":false},
                    {"name":"s","case_sensitive":false,"columntype":{"nullable":true,"precision":-1,"type":"VARCHAR"},"unused":false}],
                "primary_key": ["id"]}"#
            );

            Ok(catalog)
        })
        .unwrap();

        let mut input_stream_handle = catalog
            .input_collection_handle(&("input_map".into()))
            .unwrap()
            .handle
            .configure_deserializer(RECORD_FORMAT.clone())
            .unwrap();
        let output_stream_handles = catalog.output_handles(&("input_map".into())).unwrap();
        output_stream_handles.enable_count.enable();

        for id in 1..=8 {
            input_stream_handle
                .insert(
                    format!(r#"{{"id": {id}, "b": true, "s": "{id}"}}"#).as_bytes(),
                    &None,
                )
                .unwrap();
        }
        input_stream_handle.flush().unwrap();
        circuit.transaction().unwrap();

        let readers = output_stream_handles.delta_handle.take_readers();
        let whole = batch_to_json(readers.whole.deref());
        assert_eq!(whole.lines().count(), 8);
        let mut parts: Vec<String> = readers
            .partitions
            .iter()
            .flat_map(|reader| {
                batch_to_json(reader.deref())
                    .lines()
                    .map(String::from)
                    .collect::<Vec<_>>()
            })
            .collect();
        parts.sort();
        let mut whole: Vec<String> = whole.lines().map(String::from).collect();
        whole.sort();
        assert_eq!(parts, whole);
        assert!(
            readers.partitions.len() > 1,
            "the workers hold the rows in parts"
        );

        // The whole reads the very batches of the parts: a copy would cost a step the size of
        // the output, which for a table's integral is the whole table.
        let addresses = |reader: &dyn SerBatchReader| {
            let mut addresses = reader
                .batches()
                .iter()
                .map(|batch| Arc::as_ptr(batch) as *const ())
                .collect::<Vec<_>>();
            addresses.sort();
            addresses
        };
        let mut part_addresses = readers
            .partitions
            .iter()
            .flat_map(|reader| addresses(reader.deref()))
            .collect::<Vec<_>>();
        part_addresses.sort();
        assert_eq!(addresses(readers.whole.deref()), part_addresses);
        assert_eq!(
            addresses(readers.partitions[0].deref()),
            addresses(readers.partitions[0].deref()),
            "a part reads the same batches each time"
        );

        let readers = output_stream_handles.delta_handle.take_readers();
        assert_eq!(batch_to_json(readers.whole.deref()), "");
        assert!(readers.partitions.is_empty());
    }
}
