use super::{DeMapHandle, DeZSetHandle, SerCollectionHandleImpl};
use crate::catalog::{InputCollectionHandle, SerBatchReaderHandle};
use crate::{Catalog, ControllerError, catalog::OutputCollectionHandles};
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
    operator::{MapHandle, ZSetHandle},
    typed_batch::BatchReader,
};
use feldera_adapterlib::catalog::CircuitCatalog;
use feldera_sqllib::{SqlString, Variant, build_string_interner};
use feldera_types::program_schema::{Relation, SqlIdentifier};
use feldera_types::serde_with_context::{
    DeserializeWithContext, SerializeWithContext, SqlSerdeConfig,
};
use std::any::TypeId;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::mem::transmute;
use std::sync::{Arc, Mutex};
use tracing::debug;

const INTERNED_STRING_RELATION_NAME: &str = "feldera_interned_strings";

pub type OutputMapping = BTreeMap<String, usize>;

/// Global mapping from the name of a stream to the ordinal of the host for its
/// output connectors.
///
/// It would be better if we could pass this in to the circuit instead of having
/// a global.
pub static OUTPUT_MAPPING: Mutex<OutputMapping> = Mutex::new(BTreeMap::new());

/// Controls which output streams a multihost pipeline gathers to their
/// assigned hosts.
///
/// Gathering a stream copies all of it to one host, so a stream that nothing
/// reads should not be gathered.  A stream that an output connector reads is
/// gathered from circuit construction ("eager").  Any other stream is gathered
/// "on request": only in the transactions that start at a step whose
/// [StepRequest::gathers](feldera_types::coordination::StepRequest::gathers)
/// lists it.
#[derive(Debug, Default)]
pub struct GatherPolicy {
    /// The streams to gather eagerly, or `None` to gather all of them eagerly.
    eager: Option<BTreeSet<String>>,

    /// The streams gathered on request, by name.
    on_request: BTreeMap<String, OnRequestGather>,
}

/// A stream that [GatherPolicy] gathers on request.
#[derive(Debug, Default)]
struct OnRequestGather {
    /// The stream's gathers.  There can be more than one, because an index
    /// gathers under the name of its view.
    enable_counts: Vec<EnableCount>,

    /// While the gather runs, the first transaction whose output has every
    /// host's rows.
    since: Option<u64>,
}

/// Which transactions' output of a stream has every host's rows.  See
/// [gather_states].
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum GatherState {
    /// Transaction number `since` and every later one, as long as the gather
    /// keeps running.
    Since(u64),

    /// No transaction, because the gather is not running.
    Stopped,
}

impl GatherState {
    /// Returns true if the output of `transaction` has every host's rows.
    pub fn is_complete(&self, transaction: u64) -> bool {
        match self {
            GatherState::Since(since) => transaction >= *since,
            GatherState::Stopped => false,
        }
    }
}

impl GatherPolicy {
    const fn new() -> Self {
        Self {
            eager: None,
            on_request: BTreeMap::new(),
        }
    }

    /// Returns true if `stream` must be gathered from the start.  `assigned`
    /// says whether the coordinator assigned `stream` to a host.
    ///
    /// A stream that the coordinator did not assign to a host is always
    /// gathered eagerly, because the coordinator cannot ask for it later.
    fn is_eager(&self, stream: &str, assigned: bool) -> bool {
        self.eager
            .as_ref()
            .is_none_or(|eager| !assigned || eager.contains(stream))
    }

    /// See [apply_gathers].
    fn apply(&mut self, requested: &BTreeSet<String>, transaction: u64) -> Vec<String> {
        let mut stopped = Vec::new();
        for (stream, gather) in &mut self.on_request {
            match (requested.contains(stream), gather.since) {
                (true, None) => {
                    for enable_count in &gather.enable_counts {
                        enable_count.enable();
                    }
                    gather.since = Some(transaction);
                    // Tests read this line; see `test_multihost.py`.
                    debug!(
                        "started gathering output stream '{stream}' at transaction {transaction}"
                    );
                }
                (false, Some(_)) => {
                    for enable_count in &gather.enable_counts {
                        enable_count.disable();
                    }
                    gather.since = None;
                    stopped.push(stream.clone());
                    // Tests read this line; see `test_multihost.py`.
                    debug!(
                        "stopped gathering output stream '{stream}' at transaction {transaction}"
                    );
                }
                (true, Some(_)) | (false, None) => (),
            }
        }
        stopped
    }

    /// See [gather_states].
    fn states(&self) -> BTreeMap<String, GatherState> {
        self.on_request
            .iter()
            .map(|(stream, gather)| {
                let state = match gather.since {
                    Some(since) => GatherState::Since(since),
                    None => GatherState::Stopped,
                };
                (stream.clone(), state)
            })
            .collect()
    }
}

/// Global [GatherPolicy], set with [OUTPUT_MAPPING] before the circuit is
/// built.
pub static GATHER_POLICY: Mutex<GatherPolicy> = Mutex::new(GatherPolicy::new());

/// Sets [GATHER_POLICY] for the circuit about to be built, so that it gathers
/// only the streams in `eager` at first (or all streams, if `eager` is `None`).
pub fn configure_gathers(eager: Option<BTreeSet<String>>) {
    *GATHER_POLICY.lock().unwrap() = GatherPolicy {
        eager,
        ..GatherPolicy::new()
    };
}

/// Starts and stops this host's on-request gathers as transaction number
/// `transaction` starts, so that exactly the streams in `requested` are
/// gathered during it.  Returns the streams whose gathers stopped.
///
/// Call this before the transaction's first step.  Each worker samples its
/// gather once per transaction, so a change at any other time would reach
/// only some workers.  Every host must pass the same `requested` for the same
/// transaction, which is why it comes from the coordinator's step request.
///
/// A name in `requested` that is not an on-request stream has no effect.
pub fn apply_gathers(requested: &BTreeSet<String>, transaction: u64) -> Vec<String> {
    GATHER_POLICY.lock().unwrap().apply(requested, transaction)
}

/// Returns the state of each stream that this host gathers on request.  A
/// stream not in the map is gathered eagerly, so every transaction's output
/// of it is complete.
pub fn gather_states() -> BTreeMap<String, GatherState> {
    GATHER_POLICY.lock().unwrap().states()
}

/// Makes this host gather `stream` on request, starting out stopped, as if a
/// multihost circuit had registered it, until the returned guard drops.
///
/// This lets a test drive [apply_gathers] from a single-host controller.
/// [GATHER_POLICY] is global to the process, so `stream` should be a name
/// that no other test uses.
#[cfg(test)]
pub(crate) fn register_on_request_gather_for_test(stream: &str) -> OnRequestGatherGuard {
    GATHER_POLICY.lock().unwrap().on_request.insert(
        stream.to_string(),
        OnRequestGather {
            enable_counts: vec![EnableCount::new()],
            since: None,
        },
    );
    OnRequestGatherGuard(stream.to_string())
}

/// See [register_on_request_gather_for_test].
#[cfg(test)]
pub(crate) struct OnRequestGatherGuard(String);

#[cfg(test)]
impl Drop for OnRequestGatherGuard {
    fn drop(&mut self) {
        GATHER_POLICY.lock().unwrap().on_request.remove(&self.0);
    }
}

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
            let stream_name = name.name();
            let assignment = OUTPUT_MAPPING.lock().unwrap().get(&stream_name).copied();
            let ordinal = assignment.unwrap_or_default();

            let (accumulated_stream, enabled_count) = stream
                .shard_workers_accumulate(hosts[ordinal].workers.clone())
                .into_parts();

            // The output gather is collaborative: every host shards its slice of
            // the view to the owning host's workers, which accumulate and emit
            // it. The accumulator's `enable_count` is per-host and is enabled
            // only when an output connector attaches, which in a multihost
            // pipeline happens only on the owning host (connectors, including
            // dynamic HTTP `listen`, are assigned to one host). A non-owning
            // host would therefore leave its accumulator disabled and send an
            // empty batch instead of its slice, silently dropping every view
            // record computed on that host.
            //
            // Enable the accumulator on every host so the gather is always
            // complete. Materialized views already get this via the integral's
            // `into_enabled_stream`; do the same for the delta gather.
            //
            // Gathering a stream that nothing reads wastes storage and memory
            // on its host, so the coordinator lists the streams that output
            // connectors read.  Every host gathers those from the start.  It
            // gathers the others only while the coordinator asks for them, for
            // example while an HTTP client reads one.
            let mut policy = GATHER_POLICY.lock().unwrap();
            if policy.is_eager(&stream_name, assignment.is_some()) {
                enabled_count.enable();
            } else {
                policy
                    .on_request
                    .entry(stream_name)
                    .or_default()
                    .enable_counts
                    .push(enabled_count.clone());
            }
            drop(policy);

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
        D: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
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
            stream.local_node_id(),
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
        D: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
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
            stream.local_node_id(),
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
        KD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<K>
            + Send
            + Sync
            + Debug
            + 'static,
        VD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<V>
            + Clone
            + Debug
            + Default
            + Send
            + Sync
            + 'static,
        UD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
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
            stream.local_node_id(),
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
        KD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<K>
            + Send
            + Sync
            + Debug
            + 'static,
        VD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<V>
            + Clone
            + Debug
            + Default
            + Send
            + Sync
            + 'static,
        UD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
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
            stream.local_node_id(),
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
                        false,
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
        D: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
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
        D: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
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
        D: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
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
        D: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
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
        D: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
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
        D: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
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
        KD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<K>
            + Send
            + Sync
            + Debug
            + 'static,
        VD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
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
        KD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<K>
            + Send
            + Sync
            + Debug
            + 'static,
        VD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
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
        materialized: bool,
        accumulate: bool,
        key_fields: &[String],
    ) -> Option<OutputCollectionHandles>
    where
        KD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<K>
            + Send
            + Sync
            + Debug
            + 'static,
        VD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
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
            materialized && accumulate,
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
        KD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<K>
            + Send
            + Sync
            + Debug
            + 'static,
        VD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
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
        KD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<K>
            + Send
            + Sync
            + Debug
            + 'static,
        VD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
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
        KD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<K>
            + Send
            + Sync
            + Debug
            + 'static,
        VD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
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
        KD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
            + SerializeWithContext<SqlSerdeConfig>
            + From<K>
            + Send
            + Sync
            + Debug
            + 'static,
        VD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
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
    use std::{io::Write, ops::Deref};

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
        input_stream_handle.flush();

        circuit.transaction().unwrap();

        let delta = batch_to_json(output_stream_handles.delta_handle.concat().deref());
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
        input_stream_handle.flush();

        circuit.transaction().unwrap();

        let delta = batch_to_json(output_stream_handles.delta_handle.concat().deref());
        assert_eq!(
            delta,
            r#"-1: {"id":1,"b":true,"i":null,"s":"1"}
1: {"id":1,"b":true,"i":null,"s":"1-modified"}
"#
        );

        // Step 3: delete an entry.

        input_stream_handle.delete(br#"2"#, &None).unwrap();
        input_stream_handle.flush();

        circuit.transaction().unwrap();

        let delta = batch_to_json(output_stream_handles.delta_handle.concat().deref());
        assert_eq!(
            delta,
            r#"-1: {"id":2,"b":true,"i":null,"s":"2"}
"#
        );
    }

    mod gather_policy {
        use std::collections::{BTreeMap, BTreeSet};

        use dbsp::operator::dynamic::accumulator::EnableCount;
        use proptest::prelude::*;

        use super::super::{GatherPolicy, GatherState, OnRequestGather};

        fn names(names: &[&str]) -> BTreeSet<String> {
            names.iter().map(|name| name.to_string()).collect()
        }

        /// A policy that gathers each of `streams` on request.  Each stream
        /// has two workers, which share one enable count, as the workers of
        /// one host do.
        fn on_request(streams: &[&str]) -> (GatherPolicy, BTreeMap<String, EnableCount>) {
            let mut policy = GatherPolicy {
                eager: Some(BTreeSet::new()),
                ..GatherPolicy::new()
            };
            let mut counts = BTreeMap::new();
            for stream in streams {
                let count = EnableCount::new();
                policy.on_request.insert(
                    stream.to_string(),
                    OnRequestGather {
                        enable_counts: vec![count.clone(), count.clone()],
                        since: None,
                    },
                );
                counts.insert(stream.to_string(), count);
            }
            (policy, counts)
        }

        #[test]
        fn is_eager() {
            // An old coordinator sends no set, so every stream is eager.
            let legacy = GatherPolicy::new();
            assert!(legacy.is_eager("connected", true));
            assert!(legacy.is_eager("unread", true));

            let policy = GatherPolicy {
                eager: Some(names(&["connected"])),
                ..GatherPolicy::new()
            };
            assert!(policy.is_eager("connected", true));
            assert!(!policy.is_eager("unread", true));

            // The coordinator cannot request a gather for a stream it did not
            // assign, so such a stream is eager.
            assert!(policy.is_eager("unassigned", false));
        }

        #[test]
        fn gather_state_is_complete() {
            assert!(!GatherState::Stopped.is_complete(0));
            assert!(!GatherState::Since(5).is_complete(4));
            assert!(GatherState::Since(5).is_complete(5));
            assert!(GatherState::Since(5).is_complete(6));
        }

        /// A gather starts in the first transaction that requests it, keeps
        /// its start while the requests continue, and stops in the first
        /// transaction that does not request it.
        #[test]
        fn apply_starts_and_stops() {
            let (mut policy, counts) = on_request(&["v"]);
            let v = &counts["v"];
            assert_eq!(policy.states()["v"], GatherState::Stopped);

            // Names that are not on-request streams have no effect.
            assert_eq!(
                policy.apply(&names(&["eager", "missing"]), 1),
                Vec::<String>::new()
            );
            assert!(!v.is_enabled());

            assert_eq!(policy.apply(&names(&["v"]), 2), Vec::<String>::new());
            assert!(v.is_enabled());
            assert_eq!(policy.states()["v"], GatherState::Since(2));

            // Requesting a running gather again changes nothing.
            policy.apply(&names(&["v"]), 3);
            assert_eq!(policy.states()["v"], GatherState::Since(2));

            assert_eq!(policy.apply(&names(&[]), 4), vec!["v".to_string()]);
            assert!(!v.is_enabled());
            assert_eq!(policy.states()["v"], GatherState::Stopped);

            // A restarted gather is complete only from its new start.
            policy.apply(&names(&["v"]), 9);
            assert_eq!(policy.states()["v"], GatherState::Since(9));
        }

        /// An output endpoint also enables its stream's gather on its own
        /// host.  Stopping the on-request gather must leave that enable
        /// alone.
        #[test]
        fn apply_keeps_other_enables() {
            let (mut policy, counts) = on_request(&["v"]);
            let v = &counts["v"];
            v.enable();
            policy.apply(&names(&["v"]), 1);
            policy.apply(&names(&[]), 2);
            assert!(v.is_enabled());
            v.disable();
            assert!(!v.is_enabled());
        }

        proptest! {
            /// Against a model: after each transaction, a stream's gather runs
            /// exactly if that transaction requested it, it is complete since
            /// the first transaction of the current run of requests, and the
            /// stopped streams are the ones whose run just ended.
            #[test]
            fn apply_matches_model(requests in prop::collection::vec(
                prop::collection::btree_set(prop::sample::select(vec!["a", "b", "c"]), 0..=3),
                1..20,
            )) {
                let (mut policy, counts) = on_request(&["a", "b"]);
                let mut model: BTreeMap<&str, Option<u64>> =
                    BTreeMap::from([("a", None), ("b", None)]);
                for (transaction, requested) in (1..).zip(requests) {
                    let requested_names =
                        requested.iter().map(|name| name.to_string()).collect();
                    let stopped = policy.apply(&requested_names, transaction);

                    let mut expected_stopped = Vec::new();
                    for (stream, since) in &mut model {
                        match (requested.contains(stream), *since) {
                            (true, None) => *since = Some(transaction),
                            (false, Some(_)) => {
                                *since = None;
                                expected_stopped.push(stream.to_string());
                            }
                            _ => (),
                        }
                    }
                    prop_assert_eq!(stopped, expected_stopped);

                    let states = policy.states();
                    for (stream, since) in &model {
                        prop_assert_eq!(counts[*stream].is_enabled(), since.is_some());
                        let expected = since.map_or(GatherState::Stopped, GatherState::Since);
                        prop_assert_eq!(states[*stream], expected);
                    }
                }
            }
        }
    }
}
