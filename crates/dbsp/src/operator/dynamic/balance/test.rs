use feldera_types::config::{StorageCacheConfig, StorageConfig, StorageOptions};
use proptest::collection::vec;
use proptest::prelude::*;
use std::{
    collections::BTreeMap,
    hash::Hasher,
    path::Path,
    time::{Duration, Instant},
};
use tempfile::tempdir;

use crate::{
    Circuit, DBSPHandle, Error as DbspError, IndexedZSetHandle, OrdIndexedZSet, OrdZSet,
    OutputHandle, RootCircuit, Runtime, RuntimeError, ZWeight,
    algebra::F64,
    circuit::{
        CircuitConfig, CircuitStorageConfig, Mode, StepSize,
        metadata::{KEY_DISTRIBUTION, MetaItem, REBALANCING_IN_PROGRESS},
        runtime::tests::with_mock_process_rss,
    },
    default_hasher,
    dynamic::{Data, DowncastTrait as _},
    operator::{
        dynamic::{
            accumulator::EnableCount,
            balance::{BalancerError, BalancerHint, PartitioningPolicy},
            sharded_accumulator::delay_remote_deliveries,
        },
        output::AccumulateOutput,
    },
    trace::{BatchReader as _, Cursor as _},
    typed_batch::{BatchReader, IndexedZSetReader, SpineSnapshot, TypedBatch},
    utils::{
        Tup2, Tup3,
        test::{init_test_logger, multihost::TestHosts},
    },
};
use anyhow::Result as AnyResult;
use proptest::prelude::Strategy;

fn balance_batch<B>(
    batch: &B,
    policy: PartitioningPolicy,
    num_shards: usize,
) -> Vec<OrdIndexedZSet<B::Key, B::Val>>
where
    B: IndexedZSetReader,
{
    let mut tuples = vec![vec![]; num_shards];

    for (key, val, weight) in batch.iter() {
        let shards = match policy {
            PartitioningPolicy::Shard => vec![key.default_hash() % num_shards as u64],
            PartitioningPolicy::Broadcast => (0..num_shards as u64).collect(),
            PartitioningPolicy::Balance => {
                let mut hasher = default_hasher();
                key.dyn_hash(&mut hasher);
                val.dyn_hash(&mut hasher);
                vec![hasher.finish() % num_shards as u64]
            }
        };

        for shard in shards.into_iter() {
            tuples[shard as usize].push(Tup2(Tup2(key.clone(), val.clone()), weight));
        }
    }

    tuples
        .into_iter()
        .map(|tuples| OrdIndexedZSet::from_tuples((), tuples))
        .collect()
}

/// The input, the persistent id of the input, the output delta and the
/// output trace of [accumulate_trace_with_balancer_test_circuit].
type AccumulateTraceTestCircuitResult = (
    IndexedZSetHandle<u64, u64>,
    String,
    OutputHandle<SpineSnapshot<OrdIndexedZSet<u64, u64>>>,
    OutputHandle<OrdIndexedZSet<u64, u64>>,
);

fn accumulate_trace_with_balancer_test_circuit(
    circuit: &mut RootCircuit,
) -> AnyResult<AccumulateTraceTestCircuitResult> {
    let (input, input_handle) = circuit.add_input_indexed_zset::<u64, u64>();
    input.set_persistent_id(Some("input"));
    let input_node_id = "input".to_string();

    let (balanced_accumulator, balanced_trace) = input.accumulate_trace_balanced();

    // This balanced accumulator has no enable-count gate of its own; the
    // output operator only consults the count to force-enable a bootstrap
    // circuit's accumulator, which this test never exercises, so a
    // permanently-enabled count keeps it out of bootstrap caching mode.
    let enable_count = EnableCount::new();
    enable_count.enable();
    let (balanced_stream_output, output_handle) =
        AccumulateOutput::<OrdIndexedZSet<u64, u64>>::new(enable_count);

    let _gid = circuit.add_sink(balanced_stream_output, &balanced_accumulator);
    // circuit.set_persistent_node_id(&gid, persistent_id);

    // (output_handle, enable_count, gid)

    // let balanced_stream_output = balanced_stream.accumulate_output();
    let balanced_trace_output = balanced_trace
        .apply(|trace| trace.ro_snapshot().consolidate())
        .output();

    Ok((
        input_handle,
        input_node_id,
        output_handle,
        balanced_trace_output,
    ))
}

type JoinTestCircuitResult = (
    IndexedZSetHandle<u64, u64>,
    IndexedZSetHandle<u64, u64>,
    String,
    String,
    OutputHandle<OrdZSet<Tup3<u64, u64, Option<u64>>>>,
);

fn join_with_balancer_test_circuit(circuit: &mut RootCircuit) -> AnyResult<JoinTestCircuitResult> {
    let (left_input, left_input_handle) = circuit.add_input_indexed_zset::<u64, u64>();
    let (right_input, right_input_handle) = circuit.add_input_indexed_zset::<u64, u64>();
    left_input.set_persistent_id(Some("left_input"));
    let left_input_node_id = "left_input".to_string();

    right_input.set_persistent_id(Some("right_input"));
    let right_input_node_id = "right_input".to_string();

    // let balanced_stream_output = balanced_stream.accumulate_output();
    let join_output_handle = left_input
        .join_balanced_inner(&right_input, |key, v1, v2| Tup3(*key, *v1, Some(*v2)))
        .set_persistent_id(Some("join_output"))
        .accumulate_integrate_trace()
        .apply(|trace| trace.ro_snapshot().consolidate())
        .output_persistent(Some("output"));

    Ok((
        left_input_handle,
        right_input_handle,
        left_input_node_id,
        right_input_node_id,
        join_output_handle,
    ))
}

// s3 = s1.join(s2), s5 =s3.join(s4)

fn left_join_with_balancer_test_circuit(
    circuit: &mut RootCircuit,
) -> AnyResult<(
    IndexedZSetHandle<u64, u64>,
    IndexedZSetHandle<u64, u64>,
    String,
    String,
    OutputHandle<OrdZSet<Tup3<u64, u64, Option<u64>>>>,
)> {
    let (left_input, left_input_handle) = circuit.add_input_indexed_zset::<u64, u64>();
    let (right_input, right_input_handle) = circuit.add_input_indexed_zset::<u64, u64>();

    let right_input = right_input.map_index(|(k, v)| (k.clone(), Some(v.clone())));

    left_input.set_persistent_id(Some("left_input"));
    let left_input_node_id = "left_input".to_string();

    right_input.set_persistent_id(Some("right_input"));
    let right_input_node_id = "right_input".to_string();

    // let balanced_stream_output = balanced_stream.accumulate_output();
    let join_output_handle = left_input
        .left_join_balanced_inner(&right_input, |key, v1, v2| Tup3(*key, *v1, *v2))
        .set_persistent_id(Some("join_output"))
        .accumulate_integrate_trace()
        .apply(|trace| trace.ro_snapshot().consolidate())
        .output_persistent(Some("output"));

    Ok((
        left_input_handle,
        right_input_handle,
        left_input_node_id,
        right_input_node_id,
        join_output_handle,
    ))
}

/// The two inputs of [circular_dependency_test_circuit], the outputs of its
/// balanced joins `s1`, `s4`, `s7` and `s8`, and the outputs of the reference
/// joins for the same streams.
type CircularDependencyTestCircuitResult = (
    IndexedZSetHandle<u64, u64>,
    IndexedZSetHandle<u64, u64>,
    OutputHandle<SpineSnapshot<OrdIndexedZSet<u64, Tup2<u64, u64>>>>,
    OutputHandle<SpineSnapshot<OrdIndexedZSet<u64, Tup2<u64, u64>>>>,
    OutputHandle<SpineSnapshot<OrdIndexedZSet<u64, Tup3<u64, u64, u64>>>>,
    OutputHandle<SpineSnapshot<OrdIndexedZSet<u64, Tup3<u64, u64, u64>>>>,
    OutputHandle<SpineSnapshot<OrdIndexedZSet<u64, Tup2<u64, u64>>>>,
    OutputHandle<SpineSnapshot<OrdIndexedZSet<u64, Tup2<u64, u64>>>>,
    OutputHandle<SpineSnapshot<OrdIndexedZSet<u64, Tup3<u64, u64, u64>>>>,
    OutputHandle<SpineSnapshot<OrdIndexedZSet<u64, Tup3<u64, u64, u64>>>>,
);

/// A circuit with a circular dependency between join clusters.
///
/// ```text
///   ...> s1     s2   s3    ...>s4   s5    s6
///   .    \   /   \  /      .    \   / \   /
///   .     \ /    \ /       .     \ /   \ /
///   .     s8    s4..........    s7    s1
///   .                                .
///   ..................................
/// ```
///
/// This circuit consists of two join clusters: s1-s2-s3 and s4-s5-s6.
/// There are no dependencies between nodes within each cluster; therefore
/// normally the clustering algorithm would put all nodes in each cluster
/// in the same layer. However this can lead to a livelock, as each cluster
/// will keep waiting for the other cluster to commit before making a scheduling
/// decision and the circuit will never commit a transaction.
///
/// To address this, the clustering algorithm identifies such circular dependencies
/// and merges clusters that form a strongly connected component of the graph into
/// a single cluster. Within this cluster, there will be two layers:
/// s2-s3-s5-s6 and s1-s4.
///
/// This circuit is used to test this behavior.
fn circular_dependency_test_circuit(
    circuit: &mut RootCircuit,
) -> AnyResult<CircularDependencyTestCircuitResult> {
    let (left_input, left_input_handle) = circuit.add_input_indexed_zset::<u64, u64>();
    let (right_input, right_input_handle) = circuit.add_input_indexed_zset::<u64, u64>();

    let s2 = left_input
        .map_index(|(k, v)| (k.clone(), v.clone()))
        .set_persistent_id(Some("s2"));
    let s3 = right_input
        .map_index(|(k, v)| (k.clone(), v.clone()))
        .set_persistent_id(Some("s3"));
    let s5 = left_input
        .map_index(|(k, v)| (k.clone(), v.clone()))
        .set_persistent_id(Some("s5"));
    let s6 = right_input
        .map_index(|(k, v)| (k.clone(), v.clone()))
        .set_persistent_id(Some("s6"));

    let s1 = s5
        .join_index_balanced_inner(&s6, |key, v1, v2| Some((*key, Tup2(*v1, *v2))))
        .set_persistent_id(Some("s1"));
    let s4 = s2
        .join_index_balanced_inner(&s3, |key, v1, v2| Some((*key, Tup2(*v1, *v2))))
        .set_persistent_id(Some("s4"));
    let s7 = s4
        .join_index_balanced_inner(&s5, |key, Tup2(v1, v2), v3| {
            Some((*key, Tup3(*v1, *v2, *v3)))
        })
        .set_persistent_id(Some("s7"));
    let s8 = s2
        .join_index_balanced_inner(&s1, |key, v1, Tup2(v2, v3)| {
            Some((*key, Tup3(*v1, *v2, *v3)))
        })
        .set_persistent_id(Some("s8"));

    let output1 = s1.accumulate_output_persistent(Some("output1"));
    let output4 = s4.accumulate_output_persistent(Some("output4"));
    let output7 = s7.accumulate_output_persistent(Some("output7"));
    let output8 = s8.accumulate_output_persistent(Some("output8"));

    // Reference outputs computed using regular joins.
    let s1_ref = s5
        .join_index(&s6, |key, v1, v2| Some((*key, Tup2(*v1, *v2))))
        .set_persistent_id(Some("s1_ref"));
    let s4_ref = s2
        .join_index(&s3, |key, v1, v2| Some((*key, Tup2(*v1, *v2))))
        .set_persistent_id(Some("s4_ref"));
    let s7_ref = s4
        .join_index(&s5, |key, Tup2(v1, v2), v3| {
            Some((*key, Tup3(*v1, *v2, *v3)))
        })
        .set_persistent_id(Some("s7_ref"));
    let s8_ref = s2
        .join_index(&s1, |key, v1, Tup2(v2, v3)| {
            Some((*key, Tup3(*v1, *v2, *v3)))
        })
        .set_persistent_id(Some("s8_ref"));

    let output1_ref = s1_ref.accumulate_output_persistent(Some("output1_ref"));
    let output4_ref = s4_ref.accumulate_output_persistent(Some("output4_ref"));
    let output7_ref = s7_ref.accumulate_output_persistent(Some("output7_ref"));
    let output8_ref = s8_ref.accumulate_output_persistent(Some("output8_ref"));

    Ok((
        left_input_handle,
        right_input_handle,
        output1,
        output4,
        output7,
        output8,
        output1_ref,
        output4_ref,
        output7_ref,
        output8_ref,
    ))
}

/// Sizes for `diluted_cluster_test_circuit`, applied as balancer hints in
/// `test_skew_survives_a_diluting_cluster`.
const NUM_FILLERS: usize = 6;
const FILLER_SIZE: usize = 1_400_000_000;
const SKEWED_SIZE: usize = 400_000_000;
const SKEWED_SKEW: f64 = 4.0;

const SKEWED_STREAM: &str = "skewed";
const BCAST_STREAM: &str = "bcast";

fn filler_stream(index: usize) -> String {
    format!("filler{index}")
}

/// A join cluster in which one skewed collection is worth repartitioning and
/// nothing else can move.
///
/// ```text
///   skewed --left_join--> bcast --join--> filler0 --join--> filler1 --join--> ... --join--> filler5
/// ```
///
/// All 8 streams form one cluster, since clusters are connected components of the
/// graph whose edges are joins.
///
/// `skewed` is the outer input of a left join, so `Balance` is its only
/// alternative to `Shard`, and choosing it forces `bcast` to `Broadcast`. The
/// fillers are large and unskewed, so `Shard` is already their optimum;
/// `filler0` and `filler1` are additionally pinned to it.
fn diluted_cluster_test_circuit(
    circuit: &mut RootCircuit,
) -> AnyResult<(
    IndexedZSetHandle<u64, u64>,
    IndexedZSetHandle<u64, u64>,
    Vec<OutputHandle<SpineSnapshot<OrdIndexedZSet<u64, u64>>>>,
    Vec<OutputHandle<SpineSnapshot<OrdIndexedZSet<u64, u64>>>>,
)> {
    let (skewed_input, skewed_input_handle) = circuit.add_input_indexed_zset::<u64, u64>();
    let (filler_input, filler_input_handle) = circuit.add_input_indexed_zset::<u64, u64>();

    let skewed = skewed_input
        .map_index(|(k, v)| (k.clone(), v.clone()))
        .set_persistent_id(Some(SKEWED_STREAM));
    let bcast = filler_input
        .map_index(|(k, v)| (k.clone(), Some(v.clone())))
        .set_persistent_id(Some(BCAST_STREAM));
    let fillers = (0..NUM_FILLERS)
        .map(|index| {
            filler_input
                .map_index(|(k, v)| (k.clone(), v.clone()))
                .set_persistent_id(Some(&filler_stream(index)))
        })
        .collect::<Vec<_>>();

    let mut balanced = vec![
        skewed.left_join_index_balanced_inner(&bcast, |key, left, right| {
            Some((*key, *left + right.unwrap_or(0)))
        }),
        bcast.join_index_balanced_inner(&fillers[0], |key, left, right| {
            Some((*key, left.unwrap_or(0) + *right))
        }),
    ];

    // Reference outputs computed using regular joins.
    let mut reference = vec![
        skewed.left_join_index(&bcast, |key, left, right| {
            Some((*key, *left + right.unwrap_or(0)))
        }),
        bcast.join_index(&fillers[0], |key, left, right| {
            Some((*key, left.unwrap_or(0) + *right))
        }),
    ];

    for pair in fillers.windows(2) {
        balanced.push(
            pair[0].join_index_balanced_inner(&pair[1], |key, left, right| {
                Some((*key, *left + *right))
            }),
        );
        reference
            .push(pair[0].join_index(&pair[1], |key, left, right| Some((*key, *left + *right))));
    }

    let balanced_outputs = balanced
        .iter()
        .enumerate()
        .map(|(index, stream)| {
            stream
                .set_persistent_id(Some(&format!("balanced{index}")))
                .accumulate_output_persistent(Some(&format!("balanced_output{index}")))
        })
        .collect();
    let reference_outputs = reference
        .iter()
        .enumerate()
        .map(|(index, stream)| {
            stream
                .set_persistent_id(Some(&format!("reference{index}")))
                .accumulate_output_persistent(Some(&format!("reference_output{index}")))
        })
        .collect();

    Ok((
        skewed_input_handle,
        filler_input_handle,
        balanced_outputs,
        reference_outputs,
    ))
}

/// Starts [accumulate_trace_with_balancer_test_circuit] on `hosts` hosts with
/// `workers` workers in total, configured for the balancer to change policies
/// freely.
fn start_accumulate_trace_with_balancer(
    workers: usize,
    hosts: usize,
) -> TestHosts<AccumulateTraceTestCircuitResult> {
    TestHosts::start_with(
        workers,
        hosts,
        |_host, config| {
            config
                .with_splitter_chunk_size_records(2)
                .with_balancer_min_absolute_improvement_threshold(0)
                .with_mode(Mode::Persistent)
        },
        accumulate_trace_with_balancer_test_circuit,
    )
}

/// Takes the output delta and the output trace of
/// [accumulate_trace_with_balancer_test_circuit] from every worker.
///
/// # Returns
///
/// The delta and the trace of each worker, in order of worker index.
#[allow(clippy::type_complexity)]
fn take_accumulate_trace_output(
    hosts: &TestHosts<AccumulateTraceTestCircuitResult>,
) -> (Vec<OrdIndexedZSet<u64, u64>>, Vec<OrdIndexedZSet<u64, u64>>) {
    let deltas = hosts.map_workers(|(_, _, output_delta, _), worker| {
        output_delta.take_from_worker(worker).unwrap().consolidate()
    });
    let traces = hosts.map_workers(|(_, _, _, output_trace), worker| {
        output_trace.take_from_worker(worker).unwrap()
    });
    (deltas, traces)
}

/// Hints `policy` for `stream` on every host.
fn set_policy<T>(hosts: &mut TestHosts<T>, stream: &str, policy: PartitioningPolicy)
where
    T: Send + 'static,
{
    let stream = stream.to_string();
    hosts.for_each(move |handle| {
        handle
            .set_balancer_hint(&stream, BalancerHint::Policy(Some(policy)))
            .unwrap()
    });
}

/// Returns the current policy of `stream`, which all of the hosts must agree
/// on.
fn current_policy<T>(hosts: &mut TestHosts<T>, stream: &str) -> PartitioningPolicy
where
    T: Send + 'static,
{
    let id = stream.to_string();
    let policies = hosts.map(move |handle| handle.get_current_balancer_policy(&id).unwrap());
    assert!(
        policies.iter().all(|policy| *policy == policies[0]),
        "the hosts disagree on the policy of {stream}: {policies:?}"
    );
    policies[0]
}

fn test_accumulate_trace_with_balancer(
    workers: usize,
    hosts: usize,
    transaction: bool,
    batches: Vec<(PartitioningPolicy, Vec<Tup2<u64, Tup2<u64, ZWeight>>>)>,
) {
    let mut test_hosts = start_accumulate_trace_with_balancer(workers, hosts);
    let input_node_id = test_hosts.result(0).1.clone();

    let mut all_tuples: Vec<Tup2<Tup2<u64, u64>, ZWeight>> = vec![];

    if transaction {
        test_hosts.start_transaction();
    }

    let mut previous_policy = PartitioningPolicy::Shard;

    for (_step, (policy, batch)) in batches.iter().enumerate() {
        //println!("step: {}", step);

        set_policy(&mut test_hosts, &input_node_id, *policy);

        let retractions = balance_batch(
            &OrdIndexedZSet::from_tuples(
                (),
                all_tuples
                    .iter()
                    .map(|Tup2(Tup2(key, val), w)| Tup2(Tup2(*key, *val), -*w))
                    .collect::<Vec<_>>(),
            ),
            previous_policy,
            workers,
        );

        // println!("retractions: {:?}", retractions);

        let insertions = balance_batch(
            &OrdIndexedZSet::from_tuples((), all_tuples.clone()),
            *policy,
            workers,
        );

        // println!("insertions: {:?}", insertions);

        let mut tuples = vec![];
        let input_handle = &test_hosts.result(0).0;
        for Tup2(key, Tup2(val, w)) in batch.iter() {
            input_handle.push(*key, (*val, *w));
            tuples.push(Tup2(Tup2(*key, *val), *w));
            all_tuples.push(Tup2(Tup2(*key, *val), *w));
        }

        let input_delta = OrdIndexedZSet::from_tuples((), tuples);
        let input_deltas = balance_batch(&input_delta, *policy, workers);

        // println!("input_deltas: {:?}", input_deltas);

        let expected_output_delta = retractions
            .into_iter()
            .zip(insertions.into_iter())
            .zip(input_deltas.into_iter())
            .map(|((retraction, insertion), input_delta)| {
                TypedBatch::merge_batches([retraction, insertion, input_delta])
            })
            .collect::<Vec<_>>();

        // println!("expected_output_delta: {:?}", expected_output_delta);

        let input_trace = OrdIndexedZSet::from_tuples((), all_tuples.clone().into_iter().collect());
        let expected_output_trace = balance_batch(&input_trace, *policy, workers);

        if transaction {
            test_hosts.step();
        } else {
            test_hosts.transaction();
        }

        if !transaction {
            let (output_delta, output_trace) = take_accumulate_trace_output(&test_hosts);
            assert_eq!(output_delta, expected_output_delta);
            assert_eq!(output_trace, expected_output_trace);
        }

        previous_policy = *policy;
    }

    if transaction {
        test_hosts.commit_transaction();
        let (output_delta, output_trace) = take_accumulate_trace_output(&test_hosts);

        let input = OrdIndexedZSet::from_tuples((), all_tuples.clone().into_iter().collect());
        let expected_output = balance_batch(&input, batches.last().unwrap().0, workers);

        assert_eq!(output_delta, expected_output);
        assert_eq!(output_trace, expected_output);
    }
}

type TestBatch = Vec<Tup2<u64, Tup2<u64, ZWeight>>>;

#[derive(Debug)]
struct JoinTestStep {
    left: TestBatch,
    right: TestBatch,

    left_policy_hint: Option<Option<PartitioningPolicy>>,
    right_policy_hint: Option<Option<PartitioningPolicy>>,

    /// None means no specific policy is expected. Skip the check.
    expected_left_policy: Option<PartitioningPolicy>,
    expected_right_policy: Option<PartitioningPolicy>,
}

/// Configures `config` for the balancer to change policies freely, with
/// storage in `path`.
fn configure(config: CircuitConfig, path: &Path) -> CircuitConfig {
    config
        .with_splitter_chunk_size_records(2)
        .with_balancer_min_absolute_improvement_threshold(0)
        .with_storage(Some(
            CircuitStorageConfig::for_config(
                StorageConfig {
                    path: path.to_string_lossy().into_owned(),
                    cache: StorageCacheConfig::default(),
                },
                StorageOptions::default(),
            )
            .unwrap(),
        ))
        .with_mode(Mode::Persistent)
}

fn test_join_with_balancer(
    workers: usize,
    hosts: usize,
    transaction: bool,
    checkpoints: bool,
    inputs: Vec<JoinTestStep>,
    left_join: bool,
    auto_rebalance: bool,
) {
    init_test_logger();

    // Can't make checkpoints mid-transaction.
    assert!(!(checkpoints && transaction));

    // The last step must have a specific policy to check.
    assert!(inputs.last().unwrap().expected_left_policy.is_some());
    assert!(inputs.last().unwrap().expected_right_policy.is_some());

    let temp = tempdir().expect("Can't create temp dir for storage");

    let constructor = if left_join {
        left_join_with_balancer_test_circuit
    } else {
        join_with_balancer_test_circuit
    };

    let mut test_hosts =
        TestHosts::start_with(workers, hosts, configure_hosts(temp.path()), constructor);
    test_hosts.for_each(move |handle| handle.set_auto_rebalance(auto_rebalance).unwrap());
    let (_, _, left_input_node_id, right_input_node_id, _) = test_hosts.result(0);
    let (left_input_node_id, right_input_node_id) =
        (left_input_node_id.clone(), right_input_node_id.clone());

    let mut all_left_tuples: Vec<Tup2<Tup2<u64, u64>, ZWeight>> = vec![];
    let mut all_right_tuples: Vec<Tup2<Tup2<u64, u64>, ZWeight>> = vec![];

    if transaction {
        test_hosts.start_transaction();
    }

    for (
        _step,
        JoinTestStep {
            left,
            right,
            left_policy_hint,
            right_policy_hint,
            expected_left_policy,
            expected_right_policy,
        },
    ) in inputs.iter().enumerate()
    {
        // println!(
        //     "step: {} (left_policy_hint: {:?}, right_policy_hint: {:?})",
        //     step, left_policy_hint, right_policy_hint
        // );

        if auto_rebalance {
            let hints = vec![
                (left_input_node_id.clone(), BalancerHint::Policy(None)),
                (right_input_node_id.clone(), BalancerHint::Policy(None)),
            ];
            test_hosts.for_each(move |handle| {
                handle.set_balancer_hints(hints.clone()).unwrap();
            });
        }

        if let Some(left_policy_hint) = left_policy_hint
            && auto_rebalance
        {
            let (id, hint) = (left_input_node_id.clone(), *left_policy_hint);
            test_hosts.for_each(move |handle| {
                handle
                    .set_balancer_hint(&id, BalancerHint::Policy(hint))
                    .unwrap()
            });
        }

        if let Some(right_policy_hint) = right_policy_hint
            && auto_rebalance
        {
            let (id, hint) = (right_input_node_id.clone(), *right_policy_hint);
            test_hosts.for_each(move |handle| {
                handle
                    .set_balancer_hint(&id, BalancerHint::Policy(hint))
                    .unwrap()
            });
        }

        // println!("insertions: {:?}", insertions);

        let (left_input_handle, right_input_handle, _, _, _) = test_hosts.result(0);
        let mut left_tuples = vec![];
        for Tup2(key, Tup2(val, w)) in left.iter() {
            left_input_handle.push(*key, (*val, *w));
            left_tuples.push(Tup2(Tup2(*key, *val), *w));
            all_left_tuples.push(Tup2(Tup2(*key, *val), *w));
        }
        let mut right_tuples = vec![];
        for Tup2(key, Tup2(val, w)) in right.iter() {
            right_input_handle.push(*key, (*val, *w));
            right_tuples.push(Tup2(Tup2(*key, *val), *w));
            all_right_tuples.push(Tup2(Tup2(*key, *val), *w));
        }

        let left_input_trace =
            OrdIndexedZSet::from_tuples((), all_left_tuples.clone().into_iter().collect());
        let right_input_trace =
            OrdIndexedZSet::from_tuples((), all_right_tuples.clone().into_iter().collect());

        if transaction {
            test_hosts.step();
        } else {
            test_hosts.transaction();
        }

        if !transaction && expected_left_policy.is_some() && expected_right_policy.is_some() {
            let expected_output = reference_join(
                &left_input_trace,
                &right_input_trace,
                expected_left_policy.unwrap(),
                expected_right_policy.unwrap(),
                workers,
                left_join,
            );

            let output_trace = collect_join_output(&test_hosts);

            let current_left_policy = current_policy(&mut test_hosts, &left_input_node_id);
            let current_right_policy = current_policy(&mut test_hosts, &right_input_node_id);

            assert_eq!(current_left_policy, expected_left_policy.unwrap());
            assert_eq!(current_right_policy, expected_right_policy.unwrap());
            assert_eq!(output_trace, expected_output);

            if checkpoints {
                // Checkpoint the circuit, and start it again from the
                // checkpoint.
                let checkpoints = test_hosts.checkpoint();
                test_hosts = test_hosts.restart(&checkpoints);
            }
        }
    }

    if transaction {
        test_hosts.commit_transaction();
        let output_trace = collect_join_output(&test_hosts);

        let left_input_trace =
            OrdIndexedZSet::from_tuples((), all_left_tuples.clone().into_iter().collect());
        let right_input_trace =
            OrdIndexedZSet::from_tuples((), all_right_tuples.clone().into_iter().collect());

        let final_left_policy = inputs.last().unwrap().expected_left_policy;
        let final_right_policy = inputs.last().unwrap().expected_right_policy;

        let expected_output = reference_join(
            &left_input_trace,
            &right_input_trace,
            final_left_policy.unwrap(),
            final_right_policy.unwrap(),
            workers,
            left_join,
        );

        assert_eq!(output_trace, expected_output);
    }
}

/// Returns the contents of an output of a circuit over all of the workers on
/// all of the hosts.
///
/// # Arguments
///
/// * `hosts` - the hosts of the circuit.
/// * `output` - takes the output of one host and consolidates it, given what
///   the circuit's constructor returned on that host.
///
/// # Returns
///
/// The weight of each record in the output.
fn consolidate_hosts<T, B>(
    hosts: &TestHosts<T>,
    output: impl Fn(&T) -> B,
) -> BTreeMap<(B::Key, B::Val), ZWeight>
where
    T: Send + 'static,
    B: IndexedZSetReader,
    B::Key: Ord,
    B::Val: Ord,
{
    let mut contents = BTreeMap::new();
    for result in hosts.results() {
        for (key, val, weight) in output(result).iter() {
            *contents.entry((key, val)).or_insert(0) += weight;
        }
    }
    contents.retain(|_, weight| *weight != 0);
    contents
}

/// Checks that each balanced join of [circular_dependency_test_circuit]
/// outputs what its reference join outputs, over all of the workers.
fn check_circular_dependency_outputs(hosts: &TestHosts<CircularDependencyTestCircuitResult>) {
    assert_eq!(
        consolidate_hosts(hosts, |result| result.2.concat().consolidate()),
        consolidate_hosts(hosts, |result| result.6.concat().consolidate())
    );
    assert_eq!(
        consolidate_hosts(hosts, |result| result.3.concat().consolidate()),
        consolidate_hosts(hosts, |result| result.7.concat().consolidate())
    );
    assert_eq!(
        consolidate_hosts(hosts, |result| result.4.concat().consolidate()),
        consolidate_hosts(hosts, |result| result.8.concat().consolidate())
    );
    assert_eq!(
        consolidate_hosts(hosts, |result| result.5.concat().consolidate()),
        consolidate_hosts(hosts, |result| result.9.concat().consolidate())
    );
}

fn test_circular_dependency(
    num_workers: usize,
    hosts: usize,
    transaction: bool,
    checkpoints: bool,
    inputs: Vec<(TestBatch, TestBatch)>,
) {
    init_test_logger();

    // Can't make checkpoints mid-transaction.
    assert!(!(checkpoints && transaction));

    let temp = tempdir().expect("Can't create temp dir for storage");

    let mut test_hosts = TestHosts::start_with(
        num_workers,
        hosts,
        configure_hosts(temp.path()),
        circular_dependency_test_circuit,
    );

    if transaction {
        test_hosts.start_transaction();
    }

    for (_step, (left, right)) in inputs.iter().enumerate() {
        // println!("step: {step}");

        // println!("insertions: {:?}", insertions);

        let (input_handle1, input_handle2, ..) = test_hosts.result(0);
        for Tup2(key, Tup2(val, w)) in left.iter() {
            input_handle1.push(*key, (*val, *w));
        }
        for Tup2(key, Tup2(val, w)) in right.iter() {
            input_handle2.push(*key, (*val, *w));
        }

        if transaction {
            test_hosts.step();
        } else {
            test_hosts.transaction();
        }

        if !transaction {
            check_circular_dependency_outputs(&test_hosts);

            if checkpoints {
                // Checkpoint the circuit, and start it again from the
                // checkpoint.
                let checkpoints = test_hosts.checkpoint();
                test_hosts = test_hosts.restart(&checkpoints);
            }
        }
    }

    if transaction {
        test_hosts.commit_transaction();
        check_circular_dependency_outputs(&test_hosts);
    }
}

fn reference_join(
    left: &OrdIndexedZSet<u64, u64>,
    right: &OrdIndexedZSet<u64, u64>,
    left_policy: PartitioningPolicy,
    right_policy: PartitioningPolicy,
    num_shards: usize,
    left_join: bool,
) -> Vec<OrdZSet<Tup3<u64, u64, Option<u64>>>> {
    let mut left_cursor = left.inner().cursor();
    let mut right_cursor = right.inner().cursor();

    let mut results = vec![vec![]; num_shards];

    while left_cursor.key_valid() {
        if right_cursor.seek_key_exact(left_cursor.key(), None) {
            while left_cursor.val_valid() {
                while right_cursor.val_valid() {
                    let shard = match (left_policy, right_policy) {
                        (PartitioningPolicy::Shard, PartitioningPolicy::Shard)
                        | (PartitioningPolicy::Shard, PartitioningPolicy::Broadcast) => {
                            left_cursor.key().default_hash() % num_shards as u64
                        }
                        (PartitioningPolicy::Broadcast, PartitioningPolicy::Shard) => {
                            right_cursor.key().default_hash() % num_shards as u64
                        }
                        (PartitioningPolicy::Broadcast, PartitioningPolicy::Balance) => {
                            let mut hasher = default_hasher();
                            right_cursor.key().dyn_hash(&mut hasher);
                            right_cursor.val().dyn_hash(&mut hasher);
                            hasher.finish() % num_shards as u64
                        }
                        (PartitioningPolicy::Balance, PartitioningPolicy::Broadcast) => {
                            let mut hasher = default_hasher();
                            left_cursor.key().dyn_hash(&mut hasher);
                            left_cursor.val().dyn_hash(&mut hasher);
                            hasher.finish() % num_shards as u64
                        }
                        _ => unreachable!(),
                    };
                    results[shard as usize].push(Tup2(
                        Tup2(
                            Tup3(
                                unsafe { *left_cursor.key().downcast::<u64>() },
                                unsafe { *left_cursor.val().downcast::<u64>() },
                                Some(unsafe { *right_cursor.val().downcast::<u64>() }),
                            ),
                            (),
                        ),
                        **left_cursor.weight() * **right_cursor.weight(),
                    ));
                    right_cursor.step_val();
                }
                right_cursor.rewind_vals();
                left_cursor.step_val();
            }
        } else if left_join {
            while left_cursor.val_valid() {
                let shard = match (left_policy, right_policy) {
                    (PartitioningPolicy::Shard, PartitioningPolicy::Shard)
                    | (PartitioningPolicy::Shard, PartitioningPolicy::Broadcast) => {
                        left_cursor.key().default_hash() % num_shards as u64
                    }
                    (PartitioningPolicy::Balance, PartitioningPolicy::Broadcast) => {
                        let mut hasher = default_hasher();
                        left_cursor.key().dyn_hash(&mut hasher);
                        left_cursor.val().dyn_hash(&mut hasher);
                        hasher.finish() % num_shards as u64
                    }
                    _ => unreachable!(),
                };
                results[shard as usize].push(Tup2(
                    Tup2(
                        Tup3(
                            unsafe { *left_cursor.key().downcast::<u64>() },
                            unsafe { *left_cursor.val().downcast::<u64>() },
                            None,
                        ),
                        (),
                    ),
                    **left_cursor.weight(),
                ));

                left_cursor.step_val();
            }
        }
        left_cursor.step_key();
    }

    results
        .into_iter()
        .map(|result| OrdZSet::from_tuples((), result))
        .collect()
}

/// 20 steps, 20 tuples each, use the same policy for all steps.
fn simple_integrate_workload(
    policy: PartitioningPolicy,
) -> Vec<(PartitioningPolicy, Vec<Tup2<u64, Tup2<u64, ZWeight>>>)> {
    let mut batches = vec![];
    for step in 0..20 {
        let mut tuples = vec![];
        for key in 0..20 {
            tuples.push(Tup2(key, Tup2(step, 1)));
        }
        batches.push((policy, tuples));
    }
    batches
}

/// 20 steps, 20 tuples each, change policy after every step in round robin manner.
fn round_robin_integrate_workload() -> Vec<(PartitioningPolicy, Vec<Tup2<u64, Tup2<u64, ZWeight>>>)>
{
    let mut batches = vec![];
    for step in 0..20 {
        let mut tuples = vec![];
        for key in 0..20 {
            tuples.push(Tup2(key, Tup2(step, 1)));
        }
        batches.push((
            PartitioningPolicy::try_from((step % 3) as u8).unwrap(),
            tuples,
        ));
    }
    batches
}

fn generate_test_indexed_zset(
    max_key: u64,
    max_val: u64,
    max_tuples: usize,
) -> impl Strategy<Value = Vec<Tup2<u64, Tup2<u64, ZWeight>>>> {
    vec(
        (0..max_key, 0..max_val).prop_map(|(x, y)| Tup2(x, Tup2(y, 1))),
        0..max_tuples,
    )
}

static VALID_JOIN_POLICIES: [(PartitioningPolicy, PartitioningPolicy); 5] = [
    (PartitioningPolicy::Shard, PartitioningPolicy::Shard),
    (PartitioningPolicy::Shard, PartitioningPolicy::Broadcast),
    (PartitioningPolicy::Broadcast, PartitioningPolicy::Balance),
    (PartitioningPolicy::Broadcast, PartitioningPolicy::Shard),
    (PartitioningPolicy::Balance, PartitioningPolicy::Broadcast),
];

static VALID_LEFT_JOIN_POLICIES: [(PartitioningPolicy, PartitioningPolicy); 3] = [
    (PartitioningPolicy::Shard, PartitioningPolicy::Shard),
    (PartitioningPolicy::Shard, PartitioningPolicy::Broadcast),
    (PartitioningPolicy::Balance, PartitioningPolicy::Broadcast),
];

fn generate_join_test_data(
    max_key: u64,
    max_val: u64,
    max_tuples: usize,
    num_batches: usize,
) -> impl Strategy<Value = Vec<JoinTestStep>> {
    vec(
        (
            generate_test_indexed_zset(max_key, max_val, max_tuples),
            generate_test_indexed_zset(max_key, max_val, max_tuples),
            (0..VALID_JOIN_POLICIES.len()),
        )
            .prop_map(|(left, right, policy_index)| JoinTestStep {
                left,
                right,
                left_policy_hint: Some(Some(VALID_JOIN_POLICIES[policy_index].0)),
                right_policy_hint: Some(Some(VALID_JOIN_POLICIES[policy_index].1)),
                expected_left_policy: Some(VALID_JOIN_POLICIES[policy_index].0),
                expected_right_policy: Some(VALID_JOIN_POLICIES[policy_index].1),
            }),
        num_batches,
    )
}

fn generate_left_join_test_data(
    max_key: u64,
    max_val: u64,
    max_tuples: usize,
    num_batches: usize,
) -> impl Strategy<Value = Vec<JoinTestStep>> {
    vec(
        (
            generate_test_indexed_zset(max_key, max_val, max_tuples),
            generate_test_indexed_zset(max_key, max_val, max_tuples),
            (0..VALID_LEFT_JOIN_POLICIES.len()),
        )
            .prop_map(|(left, right, policy_index)| JoinTestStep {
                left,
                right,
                left_policy_hint: Some(Some(VALID_LEFT_JOIN_POLICIES[policy_index].0)),
                right_policy_hint: Some(Some(VALID_LEFT_JOIN_POLICIES[policy_index].1)),
                expected_left_policy: Some(VALID_LEFT_JOIN_POLICIES[policy_index].0),
                expected_right_policy: Some(VALID_LEFT_JOIN_POLICIES[policy_index].1),
            }),
        num_batches,
    )
}

/// Generate a batch with the specified size and one partition that is `skew` times the average size.
fn generate_skewed_batch(num_partitions: usize, size: usize, skew: usize) -> TestBatch {
    // Handle edge cases
    if size == 0 || num_partitions == 0 {
        return Vec::new();
    }

    // Calculate target partition sizes
    let avg_size = size / num_partitions;
    let largest_size = (skew * avg_size).min(size); // Ensure largest_size doesn't exceed size
    let remaining_size = size - largest_size;

    // Distribute remaining entries across other partitions
    let other_partitions = num_partitions - 1;
    let base_other_size = if other_partitions > 0 {
        remaining_size / other_partitions
    } else {
        0
    };
    let extra_other_size = if other_partitions > 0 {
        remaining_size % other_partitions
    } else {
        0
    };

    // Target sizes for each partition: partition 0 gets largest_size, others get base_other_size (+1 for first extra_other_size partitions)
    let mut target_sizes = vec![0; num_partitions];
    target_sizes[0] = largest_size;
    for i in 1..num_partitions {
        target_sizes[i] = base_other_size + if i <= extra_other_size { 1 } else { 0 };
    }

    // Generate keys that hash to the appropriate partitions
    let mut result = Vec::with_capacity(size);
    let mut partition_counts = vec![0; num_partitions];
    let mut key_candidate = 0u64;

    // Helper function to get partition for a key
    let get_partition = |k: u64| -> usize { (k.default_hash() % num_partitions as u64) as usize };

    // Generate keys for each partition
    for partition in 0..num_partitions {
        while partition_counts[partition] < target_sizes[partition] {
            // Find a key that hashes to this partition
            loop {
                let partition_for_key = get_partition(key_candidate);
                if partition_for_key == partition {
                    // Found a key for this partition
                    result.push(Tup2(key_candidate, Tup2(key_candidate, 1))); // Use key as value too
                    partition_counts[partition] += 1;
                    key_candidate += 1;
                    break;
                }
                key_candidate += 1;
            }
        }
    }

    result
}

#[test]
fn test_accumulate_trace_with_balancer_shard_small_step() {
    test_accumulate_trace_with_balancer(
        4,
        1,
        false,
        simple_integrate_workload(PartitioningPolicy::Shard),
    );
}

#[test]
fn test_accumulate_trace_with_balancer_bcast_small_step() {
    test_accumulate_trace_with_balancer(
        4,
        1,
        false,
        simple_integrate_workload(PartitioningPolicy::Broadcast),
    );
}

#[test]
fn test_accumulate_trace_with_balancer_balance_small_step() {
    test_accumulate_trace_with_balancer(
        4,
        1,
        false,
        simple_integrate_workload(PartitioningPolicy::Balance),
    );
}

#[test]
fn test_accumulate_trace_with_balancer_shard_big_step() {
    test_accumulate_trace_with_balancer(
        4,
        1,
        true,
        simple_integrate_workload(PartitioningPolicy::Shard),
    );
}

#[test]
fn test_accumulate_trace_with_balancer_bcast_big_step() {
    test_accumulate_trace_with_balancer(
        4,
        1,
        true,
        simple_integrate_workload(PartitioningPolicy::Broadcast),
    );
}

#[test]
fn test_accumulate_trace_with_balancer_balance_big_step() {
    test_accumulate_trace_with_balancer(
        4,
        1,
        true,
        simple_integrate_workload(PartitioningPolicy::Balance),
    );
}

#[test]
fn test_accumulate_trace_with_balancer_round_robin_small_step() {
    test_accumulate_trace_with_balancer(4, 1, false, round_robin_integrate_workload());
}

#[test]
fn test_accumulate_trace_with_balancer_round_robin_big_step() {
    test_accumulate_trace_with_balancer(4, 1, true, round_robin_integrate_workload());
}

#[test]
fn test_accumulate_trace_with_balancer_shard_small_step_multihost() {
    test_accumulate_trace_with_balancer(
        4,
        2,
        false,
        simple_integrate_workload(PartitioningPolicy::Shard),
    );
}

#[test]
fn test_accumulate_trace_with_balancer_bcast_small_step_multihost() {
    test_accumulate_trace_with_balancer(
        4,
        2,
        false,
        simple_integrate_workload(PartitioningPolicy::Broadcast),
    );
}

#[test]
fn test_accumulate_trace_with_balancer_balance_small_step_multihost() {
    test_accumulate_trace_with_balancer(
        4,
        2,
        false,
        simple_integrate_workload(PartitioningPolicy::Balance),
    );
}

#[test]
fn test_accumulate_trace_with_balancer_shard_big_step_multihost() {
    test_accumulate_trace_with_balancer(
        4,
        2,
        true,
        simple_integrate_workload(PartitioningPolicy::Shard),
    );
}

#[test]
fn test_accumulate_trace_with_balancer_bcast_big_step_multihost() {
    test_accumulate_trace_with_balancer(
        4,
        2,
        true,
        simple_integrate_workload(PartitioningPolicy::Broadcast),
    );
}

#[test]
fn test_accumulate_trace_with_balancer_balance_big_step_multihost() {
    test_accumulate_trace_with_balancer(
        4,
        2,
        true,
        simple_integrate_workload(PartitioningPolicy::Balance),
    );
}

#[test]
fn test_accumulate_trace_with_balancer_round_robin_small_step_multihost() {
    test_accumulate_trace_with_balancer(4, 2, false, round_robin_integrate_workload());
}

#[test]
fn test_accumulate_trace_with_balancer_round_robin_big_step_multihost() {
    test_accumulate_trace_with_balancer(4, 2, true, round_robin_integrate_workload());
}

/// The integral is rebalanced at commit using the policy captured on the first
/// policy change in the transaction. A long transaction can still see later
/// policy changes before commit; if the policy changes Shard -> Broadcast -> Shard,
/// the final policy matches the original trace policy. This used to panic when
/// the sender assumed that the saved trace policy and current policy must differ.
///
/// # Arguments
///
/// * `hosts` - the number of hosts to run the circuit on.
fn policy_returns_to_trace_policy(hosts: usize) {
    let workers = 4;
    let mut test_hosts = start_accumulate_trace_with_balancer(workers, hosts);
    let input_node_id = test_hosts.result(0).1.clone();

    set_policy(&mut test_hosts, &input_node_id, PartitioningPolicy::Shard);
    test_hosts.result(0).0.push(0, (0, 1));
    test_hosts.transaction();
    take_accumulate_trace_output(&test_hosts);

    test_hosts.start_transaction();

    set_policy(
        &mut test_hosts,
        &input_node_id,
        PartitioningPolicy::Broadcast,
    );
    test_hosts.result(0).0.push(1, (0, 1));
    test_hosts.step();

    set_policy(&mut test_hosts, &input_node_id, PartitioningPolicy::Shard);
    test_hosts.result(0).0.push(2, (0, 1));
    test_hosts.step();

    test_hosts.commit_transaction();

    let expected_output_delta = balance_batch(
        &OrdIndexedZSet::from_tuples((), vec![Tup2(Tup2(1, 0), 1), Tup2(Tup2(2, 0), 1)]),
        PartitioningPolicy::Shard,
        workers,
    );
    let expected_output_trace = balance_batch(
        &OrdIndexedZSet::from_tuples(
            (),
            vec![
                Tup2(Tup2(0, 0), 1),
                Tup2(Tup2(1, 0), 1),
                Tup2(Tup2(2, 0), 1),
            ],
        ),
        PartitioningPolicy::Shard,
        workers,
    );
    let (output_delta, output_trace) = take_accumulate_trace_output(&test_hosts);

    assert_eq!(
        current_policy(&mut test_hosts, &input_node_id),
        PartitioningPolicy::Shard
    );
    assert_eq!(output_delta, expected_output_delta);
    assert_eq!(output_trace, expected_output_trace);
}

#[test]
fn test_accumulate_trace_with_balancer_policy_returns_to_trace_policy() {
    policy_returns_to_trace_policy(1);
}

#[test]
fn test_accumulate_trace_with_balancer_policy_returns_to_trace_policy_multihost() {
    policy_returns_to_trace_policy(2);
}

/// A `Broadcast` policy hint on the outer (left) input of a left join cannot be
/// honored: the left side of a left join must be Shard or Balance. The hint must
/// be rejected when it is installed, rather than accepted and then crashing the
/// circuit with `NoSolution` on the first step.
///
/// Regression test: `set_hint` used to validate the cluster *before* persisting
/// the hint, so it checked a still-satisfiable (hint-free) instance, accepted the
/// hint, and the first step then solved the now-unsatisfiable instance and
/// panicked in `solve_all_clusters().unwrap()`. This is the crash produced by a
/// SQL `/*+ broadcast(pt) */` hint where `pt` is the outer input of a left join.
#[test]
fn test_reject_broadcast_hint_on_left_join_outer_input() {
    init_test_logger();

    let workers = 4;
    let (
        mut circuit,
        (left_input_handle, right_input_handle, left_input_node_id, _right_input_node_id, _output),
    ) = Runtime::init_circuit(
        CircuitConfig::from(workers)
            .with_splitter_chunk_size_records(2)
            .with_balancer_min_absolute_improvement_threshold(0)
            .with_mode(Mode::Persistent),
        left_join_with_balancer_test_circuit,
    )
    .unwrap();

    // Broadcasting the outer input of a left join is infeasible, so the hint must
    // be rejected here instead of being accepted and crashing at the first step.
    let result = circuit.set_balancer_hint(
        &left_input_node_id,
        BalancerHint::Policy(Some(PartitioningPolicy::Broadcast)),
    );
    assert!(
        result.is_err(),
        "broadcast hint on the outer input of a left join must be rejected, got {result:?}"
    );

    // With the hint rejected and rolled back, the circuit runs normally instead of
    // crashing with `NoSolution` on the first step.
    left_input_handle.push(1, (1, 1));
    right_input_handle.push(1, (1, 1));
    circuit.transaction().unwrap();
}

/// Adaptive joins exchange data through the streaming exchange, so they also
/// need it.  Without it, `join_balanced` builds an ordinary join, which does
/// not register with the balancer.
#[test]
fn test_adaptive_joins_need_streaming_exchange() {
    init_test_logger();

    for (streaming_exchange, adaptive) in [(true, true), (false, false)] {
        let mut config = CircuitConfig::with_workers(4).with_streaming_exchange(streaming_exchange);
        config.dev_tweaks.adaptive_joins = Some(true);
        let (mut circuit, (left_input_handle, right_input_handle, output_handle)) =
            Runtime::init_circuit(config, |circuit| {
                let (left_input, left_input_handle) = circuit.add_input_indexed_zset::<u64, u64>();
                let (right_input, right_input_handle) =
                    circuit.add_input_indexed_zset::<u64, u64>();
                let output_handle = left_input
                    .join_balanced(&right_input, |key, v1, v2| Tup3(*key, *v1, Some(*v2)))
                    .accumulate_output();
                Ok((left_input_handle, right_input_handle, output_handle))
            })
            .unwrap();
        assert_eq!(
            circuit.adaptive_joins_enabled(),
            adaptive,
            "streaming_exchange={streaming_exchange}"
        );

        left_input_handle.push(1, (1, 1));
        right_input_handle.push(1, (2, 1));
        circuit.transaction().unwrap();
        assert_eq!(
            SpineSnapshot::<OrdZSet<Tup3<u64, u64, Option<u64>>>>::concat(
                &output_handle.take_from_all()
            )
            .iter()
            .collect::<Vec<_>>(),
            vec![(Tup3(1, 1, Some(2)), (), 1)],
            "streaming_exchange={streaming_exchange}"
        );
        assert_eq!(
            !circuit.get_current_balancer_policies().unwrap().is_empty(),
            adaptive,
            "streaming_exchange={streaming_exchange}"
        );
        circuit.kill().unwrap();
    }
}

/// Adaptive joins do not support full steps, so the runtime refuses to start a
/// circuit that runs in full steps with adaptive joins enabled, and starts one
/// without them.
#[test]
fn test_adaptive_joins_reject_full_steps() {
    init_test_logger();

    for adaptive_joins in [false, true] {
        let mut config = CircuitConfig::with_workers(4).with_step_size(StepSize::FullSteps);
        config.dev_tweaks.adaptive_joins = Some(adaptive_joins);
        let result = Runtime::init_circuit(config, |_circuit| Ok(()));
        if adaptive_joins {
            assert!(matches!(
                result,
                Err(DbspError::Runtime(RuntimeError::AdaptiveJoinsInFullSteps))
            ));
        } else {
            result.unwrap().0.kill().unwrap();
        }
    }
}

/// Returns a function that configures each host of a [TestHosts] as
/// [configure] does, with storage in a directory of its own under `path`.
fn configure_hosts(path: &Path) -> impl Fn(usize, CircuitConfig) -> CircuitConfig + use<> {
    let path = path.to_path_buf();
    move |host, config| {
        let host_path = path.join(format!("host{host}"));
        std::fs::create_dir_all(&host_path).unwrap();
        configure(config, &host_path)
    }
}

/// The worker that [join_with_slow_worker_test_circuit] slows down.
const SLOW_WORKER: usize = 3;

/// Like [join_with_balancer_test_circuit], but [SLOW_WORKER] evaluates the
/// left input late in every step that has input.  The other workers
/// therefore send their part of such a step, including one in which the
/// policy changes, before it does.
fn join_with_slow_worker_test_circuit(
    circuit: &mut RootCircuit,
) -> AnyResult<JoinTestCircuitResult> {
    let (left_input, left_input_handle) = circuit.add_input_indexed_zset::<u64, u64>();
    let (right_input, right_input_handle) = circuit.add_input_indexed_zset::<u64, u64>();

    let left = left_input.apply(|batch| {
        if !batch.is_empty() && Runtime::worker_index() == SLOW_WORKER {
            std::thread::sleep(Duration::from_millis(5));
        }
        batch.clone()
    });
    left.set_persistent_id(Some("left_input"));
    right_input.set_persistent_id(Some("right_input"));

    let join_output_handle = left
        .join_balanced_inner(&right_input, |key, v1, v2| Tup3(*key, *v1, Some(*v2)))
        .set_persistent_id(Some("join_output"))
        .accumulate_integrate_trace()
        .apply(|trace| trace.ro_snapshot().consolidate())
        .output_persistent(Some("output"));

    Ok((
        left_input_handle,
        right_input_handle,
        "left_input".to_string(),
        "right_input".to_string(),
        join_output_handle,
    ))
}

/// How late [join_with_late_delivery_test_circuit] delivers each message
/// from a remote host.
const LATE_DELIVERY: Duration = Duration::from_millis(100);

/// Like [join_with_balancer_test_circuit], but each host delivers every
/// message from a remote host [LATE_DELIVERY] late (see
/// [delay_remote_deliveries]).  A step takes less time than the deliveries
/// of its messages, so a host falls further behind with every step.
fn join_with_late_delivery_test_circuit(
    circuit: &mut RootCircuit,
) -> AnyResult<JoinTestCircuitResult> {
    delay_remote_deliveries(&Runtime::runtime().unwrap(), LATE_DELIVERY);
    join_with_balancer_test_circuit(circuit)
}

/// Policies for the left and right inputs of a join.
type JoinPolicies = (PartitioningPolicy, PartitioningPolicy);

/// Hints the join's inputs on every host to use `policies`.
fn set_join_policies<T>(hosts: &mut TestHosts<T>, left: &str, right: &str, policies: JoinPolicies)
where
    T: Send + 'static,
{
    let (left, right) = (left.to_string(), right.to_string());
    hosts.for_each(move |handle| {
        // Clear both hints first, so that no intermediate pair of hints is
        // invalid.
        for result in handle
            .set_balancer_hints(vec![
                (left.clone(), BalancerHint::Policy(None)),
                (right.clone(), BalancerHint::Policy(None)),
            ])
            .unwrap()
        {
            result.unwrap();
        }
        handle
            .set_balancer_hint(&left, BalancerHint::Policy(Some(policies.0)))
            .unwrap();
        handle
            .set_balancer_hint(&right, BalancerHint::Policy(Some(policies.1)))
            .unwrap();
    });
}

/// Returns the total weight that the exchange senders of a circuit's balanced
/// inputs have counted in their key distributions, over all workers.
///
/// # Arguments
///
/// * `handles` - one handle for each host of the circuit.
fn key_distribution_total(handles: &mut [DBSPHandle]) -> ZWeight {
    let mut total = 0;
    for handle in handles {
        for worker in handle.retrieve_profile().unwrap().worker_profiles {
            for sizes in worker.attribute_profile(&KEY_DISTRIBUTION).into_values() {
                let MetaItem::Array(sizes) = sizes else {
                    panic!("a key distribution should be an array: {sizes:?}");
                };
                for size in sizes {
                    let MetaItem::Count(size) = size else {
                        panic!("a shard size should be a count: {size:?}");
                    };
                    // `KeyDistribution::meta_item` casts the signed size to
                    // `usize`, and this casts it back.
                    total += size as ZWeight;
                }
            }
        }
    }
    total
}

/// Takes the join's output from every worker, in order of worker index.
///
/// # Arguments
///
/// * `hosts` - the hosts of a circuit that [join_with_balancer_test_circuit]
///   or a variant of it builds.
///
/// # Returns
///
/// The output of each worker.
fn collect_join_output(
    hosts: &TestHosts<JoinTestCircuitResult>,
) -> Vec<OrdZSet<Tup3<u64, u64, Option<u64>>>> {
    hosts.map_workers(|(_, _, _, _, output_handle), worker| {
        output_handle.take_from_worker(worker).unwrap()
    })
}

/// Changes a join's policies in the middle of transactions, several times in
/// one transaction, and while a transaction commits, and checks the join's
/// output on every worker and the total of the key distributions after every
/// transaction.
///
/// Every policy change exercises the take race: when the policy changes, a
/// worker must redistribute what it has received in the transaction, but
/// faster workers may already have sent it their part of the step.
///
/// # Arguments
///
/// * `test_hosts` - the hosts of a circuit that
///   [join_with_slow_worker_test_circuit] builds.
fn check_join_with_policy_changes(test_hosts: &mut TestHosts<JoinTestCircuitResult>) {
    use PartitioningPolicy::{Balance, Broadcast, Shard};

    let hosts = test_hosts.n_hosts();
    let workers: usize = (0..hosts).map(|host| test_hosts.workers(host).len()).sum();

    // For each transaction, the policies to hint before each step, and before
    // the commit.
    let schedule: [(Vec<Option<JoinPolicies>>, Option<JoinPolicies>); 5] = [
        (
            vec![None, Some((Shard, Broadcast)), None, None],
            Some((Broadcast, Balance)),
        ),
        (
            vec![
                None,
                Some((Broadcast, Shard)),
                Some((Balance, Broadcast)),
                None,
            ],
            Some((Shard, Shard)),
        ),
        (vec![None, None, None, None], Some((Balance, Broadcast))),
        (
            vec![
                Some((Broadcast, Balance)),
                None,
                Some((Shard, Broadcast)),
                None,
            ],
            Some((Broadcast, Shard)),
        ),
        (vec![None, Some((Shard, Shard)), None, None], None),
    ];

    let (_, _, left_id, right_id, _) = test_hosts.result(0);
    let (left_id, right_id) = (left_id.clone(), right_id.clone());

    let mut policies = (Shard, Shard);
    let mut all_left_tuples: Vec<Tup2<Tup2<u64, u64>, ZWeight>> = vec![];
    let mut all_right_tuples: Vec<Tup2<Tup2<u64, u64>, ZWeight>> = vec![];
    let mut inserted_left = vec![];
    let mut n_retracted = 0;
    let mut next_val = 0;
    for (transaction, (steps, commit_policies)) in schedule.into_iter().enumerate() {
        test_hosts.start_transaction();
        for step_policies in steps {
            if let Some(step_policies) = step_policies {
                set_join_policies(test_hosts, &left_id, &right_id, step_policies);
                policies = step_policies;
            }

            let (left_input_handle, right_input_handle, _, _, _) = test_hosts.result(0);
            for key in 0..8 {
                left_input_handle.push(key, (next_val, 1));
                all_left_tuples.push(Tup2(Tup2(key, next_val), 1));
                inserted_left.push((key, next_val));
                if key % 2 == next_val % 2 {
                    right_input_handle.push(key, (next_val, 1));
                    all_right_tuples.push(Tup2(Tup2(key, next_val), 1));
                }
                next_val += 1;
            }
            // Retract the oldest left record that is still present.
            let (key, val) = inserted_left[n_retracted];
            n_retracted += 1;
            left_input_handle.push(key, (val, -1));
            all_left_tuples.push(Tup2(Tup2(key, val), -1));

            test_hosts.step();
        }
        if let Some(commit_policies) = commit_policies {
            set_join_policies(test_hosts, &left_id, &right_id, commit_policies);
            policies = commit_policies;
        }
        test_hosts.commit_transaction();

        let output = collect_join_output(test_hosts);
        let expected_output = reference_join(
            &OrdIndexedZSet::from_tuples((), all_left_tuples.clone()),
            &OrdIndexedZSet::from_tuples((), all_right_tuples.clone()),
            policies.0,
            policies.1,
            workers,
            false,
        );
        assert_eq!(
            output, expected_output,
            "transaction {transaction} with {workers} workers on {hosts} hosts"
        );

        // A sender counts every record of its input once, whether it sends
        // the record in the record's step or saves it for the rebalancing.
        let input_weight: ZWeight = all_left_tuples
            .iter()
            .chain(&all_right_tuples)
            .map(|Tup2(_, weight)| weight)
            .sum();
        assert_eq!(
            key_distribution_total(test_hosts.handles()),
            input_weight,
            "transaction {transaction} with {workers} workers on {hosts} hosts"
        );

        assert_eq!(
            test_hosts
                .handle(0)
                .get_current_balancer_policy(&left_id)
                .unwrap(),
            policies.0
        );
        assert_eq!(
            test_hosts
                .handle(0)
                .get_current_balancer_policy(&right_id)
                .unwrap(),
            policies.1
        );
    }
}

/// Runs [check_join_with_policy_changes] on `hosts` hosts with `workers`
/// workers in all, with [SLOW_WORKER] evaluating late.
fn test_join_with_policy_changes(workers: usize, hosts: usize) {
    init_test_logger();

    let temp = tempdir().expect("Can't create temp dir for storage");
    let mut test_hosts = TestHosts::start_with(
        workers,
        hosts,
        configure_hosts(temp.path()),
        join_with_slow_worker_test_circuit,
    );
    check_join_with_policy_changes(&mut test_hosts);
}

#[test]
fn test_join_with_policy_changes_single_host() {
    test_join_with_policy_changes(4, 1);
}

#[test]
fn test_join_with_policy_changes_multihost() {
    test_join_with_policy_changes(4, 2);
}

/// Changes a join's policy on two hosts in the step after one that sent
/// data to remote workers, while that data is still in flight.  Each worker
/// must wait for the data before it takes what it has received (see
/// `ShardedAccumulator::wait_for_arrival`).  Otherwise, the rebalancing
/// misses the data, which stays where the old policy sent it.
#[test]
fn test_policy_change_with_late_delivery() {
    use PartitioningPolicy::{Broadcast, Shard};

    init_test_logger();

    let workers = 4;
    let temp = tempdir().expect("Can't create temp dir for storage");
    let mut test_hosts = TestHosts::start_with(
        workers,
        2,
        configure_hosts(temp.path()),
        join_with_late_delivery_test_circuit,
    );
    let (_, _, left_id, right_id, _) = test_hosts.result(0);
    let (left_id, right_id) = (left_id.clone(), right_id.clone());
    let mut all_left: Vec<Tup2<Tup2<u64, u64>, ZWeight>> = vec![];
    let mut all_right: Vec<Tup2<Tup2<u64, u64>, ZWeight>> = vec![];

    // The first step broadcasts the left input to every worker on both
    // hosts.  The second step changes the left policy.
    test_hosts.start_transaction();
    for (step, policies) in [(Broadcast, Shard), (Shard, Shard)].into_iter().enumerate() {
        set_join_policies(&mut test_hosts, &left_id, &right_id, policies);
        let (left_input_handle, right_input_handle, _, _, _) = test_hosts.result(0);
        for key in 0..8 {
            let val = step as u64 * 8 + key;
            left_input_handle.push(key, (val, 1));
            all_left.push(Tup2(Tup2(key, val), 1));
            right_input_handle.push(key, (val, 1));
            all_right.push(Tup2(Tup2(key, val), 1));
        }
        test_hosts.step();
    }
    test_hosts.commit_transaction();

    assert_eq!(
        collect_join_output(&test_hosts),
        reference_join(
            &OrdIndexedZSet::from_tuples((), all_left),
            &OrdIndexedZSet::from_tuples((), all_right),
            Shard,
            Shard,
            workers,
            false,
        )
    );
}

/// Like [test_join_with_policy_changes_single_host], but under high memory
/// pressure, which makes a worker move the input that it saves for the
/// rebalancing to storage (see `RebalancingExchangeSender::defer_delta`).
#[test]
fn test_join_with_policy_changes_under_memory_pressure() {
    const GIB: u64 = 1 << 30;

    init_test_logger();

    // 9.4 GiB of a 10 GiB limit is past the 90% where high pressure starts
    // and short of the 95% where critical pressure starts.
    with_mock_process_rss(94 * GIB / 10, || {
        let temp = tempdir().expect("Can't create temp dir for storage");
        let path = temp.path().to_path_buf();
        let mut test_hosts = TestHosts::start_with(
            4,
            1,
            move |_host, config| configure(config, &path).with_max_rss_bytes(Some(10 * GIB)),
            join_with_slow_worker_test_circuit,
        );

        // The runtime samples the process size once a second.  Under high
        // pressure, it moves every new batch to storage.
        let deadline = Instant::now() + Duration::from_secs(30);
        while test_hosts.handle(0).runtime().min_insert_storage_bytes() != Some(0) {
            assert!(
                Instant::now() < deadline,
                "memory pressure did not become high"
            );
            std::thread::sleep(Duration::from_millis(100));
        }

        check_join_with_policy_changes(&mut test_hosts);
    });
}

/// Returns the number of workers in which an input of a join is rebalancing
/// at commit (see `FlushState::RebalanceInProgress`).
///
/// # Arguments
///
/// * `handle` - the handle of a circuit on a single host.
fn workers_rebalancing(handle: &mut DBSPHandle) -> usize {
    handle
        .retrieve_profile()
        .unwrap()
        .worker_profiles
        .iter()
        .filter(|worker| {
            worker
                .attribute_profile(&REBALANCING_IN_PROGRESS)
                .values()
                .any(|rebalancing| matches!(rebalancing, MetaItem::Bool(true)))
        })
        .count()
}

/// Hints a new policy for a join's input while the input rebalances at
/// commit, after some workers have finished rebalancing it and before one
/// has.  A worker that has finished can no longer change its policy in the
/// transaction, so the balancer must reject the hint until the next one.
///
/// # Arguments
///
/// * `hosts` - the number of hosts to run the join on.  On more than one
///   host, the workers that have finished can be on a host other than the
///   one that still rebalances.
fn policy_frozen_after_flush(hosts: usize) {
    use PartitioningPolicy::{Broadcast, Shard};

    init_test_logger();

    let workers = 4;
    let temp = tempdir().expect("Can't create temp dir for storage");
    let mut test_hosts = TestHosts::start_with(
        workers,
        hosts,
        configure_hosts(temp.path()),
        join_with_balancer_test_circuit,
    );
    let (_, _, left_id, right_id, _) = test_hosts.result(0);
    let (left_id, right_id) = (left_id.clone(), right_id.clone());
    let mut all_left: Vec<Tup2<Tup2<u64, u64>, ZWeight>> = vec![];
    let mut all_right: Vec<Tup2<Tup2<u64, u64>, ZWeight>> = vec![];

    // Every record has the same key, so under `Shard` one worker holds the
    // whole left integral.  Rebalancing it takes that worker many more steps
    // than the others, which have little or nothing to send.
    set_join_policies(&mut test_hosts, &left_id, &right_id, (Shard, Shard));
    let (left_input_handle, right_input_handle, _, _, _) = test_hosts.result(0);
    for val in 0..20 {
        left_input_handle.push(0, (val, 1));
        all_left.push(Tup2(Tup2(0, val), 1));
    }
    right_input_handle.push(0, (0, 1));
    all_right.push(Tup2(Tup2(0, 0), 1));
    test_hosts.transaction();

    // Change the left policy in a transaction of more than one step, so that
    // its commit rebalances the left integral.
    test_hosts.start_transaction();
    set_join_policies(&mut test_hosts, &left_id, &right_id, (Broadcast, Shard));
    for val in 20..22 {
        test_hosts.result(0).0.push(0, (val, 1));
        all_left.push(Tup2(Tup2(0, val), 1));
        test_hosts.step();
    }
    test_hosts.for_each(|handle| handle.start_commit_transaction().unwrap());
    let mut hinted = false;
    while !test_hosts.step() {
        let rebalancing: usize = test_hosts
            .handles()
            .iter_mut()
            .map(workers_rebalancing)
            .sum();
        if !hinted && (1..workers).contains(&rebalancing) {
            let id = left_id.clone();
            for result in test_hosts
                .map(move |handle| handle.set_balancer_hint(&id, BalancerHint::Policy(Some(Shard))))
            {
                assert!(
                    matches!(
                        result,
                        Err(DbspError::Balancer(BalancerError::InvalidPolicyHint(
                            Shard,
                            _
                        )))
                    ),
                    "a hint while {rebalancing} of {workers} workers rebalance must be rejected, got {result:?}"
                );
            }
            hinted = true;
        }
    }
    assert!(
        hinted,
        "the commit had no step in which only some workers rebalanced"
    );
    assert_eq!(current_policy(&mut test_hosts, &left_id), Broadcast);
    assert_eq!(
        collect_join_output(&test_hosts),
        reference_join(
            &OrdIndexedZSet::from_tuples((), all_left.clone()),
            &OrdIndexedZSet::from_tuples((), all_right.clone()),
            Broadcast,
            Shard,
            workers,
            false,
        )
    );

    // The next transaction can change the policy again.
    set_join_policies(&mut test_hosts, &left_id, &right_id, (Shard, Shard));
    test_hosts.transaction();
    assert_eq!(current_policy(&mut test_hosts, &left_id), Shard);
    assert_eq!(
        collect_join_output(&test_hosts),
        reference_join(
            &OrdIndexedZSet::from_tuples((), all_left),
            &OrdIndexedZSet::from_tuples((), all_right),
            Shard,
            Shard,
            workers,
            false,
        )
    );
}

#[test]
fn test_policy_frozen_after_flush() {
    policy_frozen_after_flush(1);
}

#[test]
fn test_policy_frozen_after_flush_multihost() {
    policy_frozen_after_flush(2);
}

/// Join a large left collection with a small right collection. Both are skewed.
/// The balancer should balance the left collection using Policy::Balance and the
/// right collection using Policy::Broadcast.
fn test_skewed_join_left(hosts: usize, left_join: bool, auto_rebalance: bool) {
    let num_partitions = 4;

    let mut test_steps = vec![];

    // Several evenly distributed batches. Expected policy: (Shard, Shard)
    for _ in 0..5 {
        let left_batch = generate_skewed_batch(num_partitions, 20, 1);
        let right_batch = generate_skewed_batch(num_partitions, 10, 1);
        test_steps.push(JoinTestStep {
            left: left_batch,
            right: right_batch,
            left_policy_hint: None,
            right_policy_hint: None,
            expected_left_policy: Some(PartitioningPolicy::Shard),
            expected_right_policy: Some(PartitioningPolicy::Shard),
        });
    }

    // Introduce skew. Expected policy: (Balance, Broadcast)

    for i in 0..10 {
        let left_batch = generate_skewed_batch(num_partitions, 100, 5);
        let right_batch = generate_skewed_batch(num_partitions, 10, 5);
        test_steps.push(JoinTestStep {
            left: left_batch,
            right: right_batch,
            left_policy_hint: None,
            right_policy_hint: None,
            expected_left_policy: if !auto_rebalance {
                Some(PartitioningPolicy::Shard)
            } else if i >= 5 {
                Some(PartitioningPolicy::Balance)
            } else {
                None
            },
            expected_right_policy: if !auto_rebalance {
                Some(PartitioningPolicy::Shard)
            } else if i >= 5 {
                Some(PartitioningPolicy::Broadcast)
            } else {
                None
            },
        });
    }
    test_join_with_balancer(
        num_partitions,
        hosts,
        false,
        false,
        test_steps,
        left_join,
        auto_rebalance,
    );
}

/// Skew that grows slowly, a few records in each step, still makes the
/// balancer change the join's policies.
///
/// # Arguments
///
/// * `hosts` - the number of hosts to run the join on.
fn slow_skew(hosts: usize) {
    let num_partitions = 4;

    let mut test_steps = vec![];

    // Several evenly distributed batches. Expected policy: (Shard, Shard)
    for _ in 0..10 {
        let left_batch = generate_skewed_batch(num_partitions, 20, 1);
        let right_batch = generate_skewed_batch(num_partitions, 10, 1);
        test_steps.push(JoinTestStep {
            left: left_batch,
            right: right_batch,
            left_policy_hint: None,
            right_policy_hint: None,
            expected_left_policy: Some(PartitioningPolicy::Shard),
            expected_right_policy: Some(PartitioningPolicy::Shard),
        });
    }

    // Introduce skew. Expected policy: (Balance, Broadcast)

    for i in 0..100 {
        let left_batch = generate_skewed_batch(num_partitions, 5, 5);
        let right_batch = generate_skewed_batch(num_partitions, 1, 1);
        test_steps.push(JoinTestStep {
            left: left_batch,
            right: right_batch,
            left_policy_hint: None,
            right_policy_hint: None,
            expected_left_policy: if i >= 80 {
                Some(PartitioningPolicy::Balance)
            } else {
                None
            },
            expected_right_policy: if i >= 80 {
                Some(PartitioningPolicy::Broadcast)
            } else {
                None
            },
        });
    }
    test_join_with_balancer(num_partitions, hosts, false, false, test_steps, false, true);
}

#[test]
fn test_slow_skew() {
    slow_skew(1);
}

#[test]
fn test_slow_skew_multihost() {
    slow_skew(2);
}

/// The skewed collection reaches `Balance` even though its cluster is too large
/// for the change to look significant cluster-wide.
///
/// # Arguments
///
/// * `hosts` - the number of hosts to run the circuit on.
fn skew_survives_a_diluting_cluster(hosts: usize) {
    init_test_logger();

    let workers = 4;
    let temp = tempdir().expect("Can't create temp dir for storage");

    let mut test_hosts = TestHosts::start_with(
        workers,
        hosts,
        configure_hosts(temp.path()),
        diluted_cluster_test_circuit,
    );
    test_hosts.for_each(|handle| handle.set_auto_rebalance(true).unwrap());

    // Size hints stand in for the filler collections, which cost too much to
    // materialize. `filler0` and `filler1` are also pinned, the other way a
    // cluster member becomes immovable. Together these make `Balance` beat
    // `Shard` 110,000,001 to 400,000,000 on the two collections the change
    // repartitions, which is only an 11.6% cut of the cluster's 2,500,000,015,
    // under the 16.7% the default 1.2 relative threshold demands.
    let mut hints = (0..NUM_FILLERS)
        .map(|index| (filler_stream(index), BalancerHint::Size(Some(FILLER_SIZE))))
        .collect::<Vec<_>>();
    hints.push((
        filler_stream(0),
        BalancerHint::Policy(Some(PartitioningPolicy::Shard)),
    ));
    hints.push((
        filler_stream(1),
        BalancerHint::Policy(Some(PartitioningPolicy::Shard)),
    ));
    test_hosts.for_each(move |handle| {
        for result in handle.set_balancer_hints(hints.clone()).unwrap() {
            result.unwrap();
        }
    });

    // Five evenly distributed steps, then five skewed ones. The even phase is what
    // makes this a test of the gate: the gate is only consulted once every stream
    // reports an effective policy that the current domains still admit.
    for step in 0..10 {
        if step == 5 {
            // The data below really is skewed onto one worker, but a collection
            // whose skew is worth billions of records is not one a unit test can
            // materialize, so scale it with hints. Setting a hint also requests a
            // refresh, which gets the cluster re-solved at the start of the next
            // transaction, while its policies can still change.
            test_hosts.for_each(|handle| {
                handle
                    .set_balancer_hint(SKEWED_STREAM, BalancerHint::Size(Some(SKEWED_SIZE)))
                    .unwrap();
                handle
                    .set_balancer_hint(
                        SKEWED_STREAM,
                        BalancerHint::Skew(Some(F64::new(SKEWED_SKEW))),
                    )
                    .unwrap();
            });
        }

        let skewed_batch = if step < 5 {
            generate_skewed_batch(workers, 20, 1)
        } else {
            generate_skewed_batch(workers, 100, 5)
        };
        let (skewed_handle, filler_handle, _, _) = test_hosts.result(0);
        for Tup2(key, Tup2(val, w)) in skewed_batch {
            skewed_handle.push(key, (val, w));
        }
        for Tup2(key, Tup2(val, w)) in generate_skewed_batch(workers, 10, 1) {
            filler_handle.push(key, (val, w));
        }

        test_hosts.transaction();

        for index in 0..test_hosts.result(0).2.len() {
            assert_eq!(
                consolidate_hosts(&test_hosts, |result| result.2[index].concat().consolidate()),
                consolidate_hosts(&test_hosts, |result| result.3[index].concat().consolidate()),
                "step {step}, join {index}"
            );
        }

        let (expected_skewed, expected_bcast) = if step < 5 {
            (PartitioningPolicy::Shard, PartitioningPolicy::Shard)
        } else {
            (PartitioningPolicy::Balance, PartitioningPolicy::Broadcast)
        };

        assert_eq!(
            current_policy(&mut test_hosts, SKEWED_STREAM),
            expected_skewed,
            "step {step}"
        );
        assert_eq!(
            current_policy(&mut test_hosts, BCAST_STREAM),
            expected_bcast,
            "step {step}"
        );

        // The fillers are what dilutes the ratio, so they must stay put.
        for index in 0..NUM_FILLERS {
            assert_eq!(
                current_policy(&mut test_hosts, &filler_stream(index)),
                PartitioningPolicy::Shard,
                "step {step}, filler {index}"
            );
        }
    }
}

#[test]
fn test_skew_survives_a_diluting_cluster() {
    skew_survives_a_diluting_cluster(1);
}

#[test]
fn test_skew_survives_a_diluting_cluster_multihost() {
    skew_survives_a_diluting_cluster(2);
}

#[test]
fn test_skewed_inner_join_left() {
    test_skewed_join_left(1, false, true);
}

#[test]
fn test_skewed_inner_join_left_multihost() {
    test_skewed_join_left(2, false, true);
}

#[test]
fn test_skewed_inner_join_left_disable_auto_rebalance() {
    test_skewed_join_left(1, false, false);
}

#[test]
fn test_skewed_inner_join_left_disable_auto_rebalance_multihost() {
    test_skewed_join_left(2, false, false);
}

#[test]
fn test_skewed_left_join_no_checkpoints() {
    test_skewed_join_left(1, true, true);
}

#[test]
fn test_skewed_left_join_no_checkpoints_multihost() {
    test_skewed_join_left(2, true, true);
}

/// Join a small left collection with a large right collection. Both are skewed.
/// The balancer should balance the left collection using Policy::Broadcast and the
/// right collection using Policy::Balance.
fn test_skewed_join_right(hosts: usize, checkpoints: bool, left_join: bool) {
    let num_partitions = 4;

    let mut test_steps = vec![];

    // Several evenly distributed batches. Expected policy: (Shard, Shard)
    for _ in 0..5 {
        let left_batch = generate_skewed_batch(num_partitions, 10, 1);
        let right_batch = generate_skewed_batch(num_partitions, 20, 1);
        test_steps.push(JoinTestStep {
            left: left_batch,
            right: right_batch,
            left_policy_hint: None,
            right_policy_hint: None,
            expected_left_policy: Some(PartitioningPolicy::Shard),
            expected_right_policy: Some(PartitioningPolicy::Shard),
        });
    }

    // Introduce skew. Expected policy: (Balance, Broadcast)

    for i in 0..10 {
        let left_batch = generate_skewed_batch(num_partitions, 10, 5);
        let right_batch = generate_skewed_batch(num_partitions, 100, 5);
        test_steps.push(JoinTestStep {
            left: left_batch,
            right: right_batch,
            left_policy_hint: None,
            right_policy_hint: None,
            expected_left_policy: if i >= 5 {
                // Left join doesn't allow Broadcast on the left side.
                if left_join {
                    Some(PartitioningPolicy::Shard)
                } else {
                    Some(PartitioningPolicy::Broadcast)
                }
            } else {
                None
            },
            expected_right_policy: if i >= 5 {
                if left_join {
                    Some(PartitioningPolicy::Shard)
                } else {
                    Some(PartitioningPolicy::Balance)
                }
            } else {
                None
            },
        });
    }
    test_join_with_balancer(
        num_partitions,
        hosts,
        false,
        checkpoints,
        test_steps,
        left_join,
        true,
    );
}

#[test]
fn test_skewed_join_right_no_checkpoints() {
    test_skewed_join_right(1, false, false);
}

#[test]
fn test_skewed_join_right_no_checkpoints_multihost() {
    test_skewed_join_right(2, false, false);
}

#[test]
fn test_skewed_join_right_checkpoints() {
    test_skewed_join_right(1, true, false);
}

#[test]
fn test_skewed_join_right_checkpoints_multihost() {
    test_skewed_join_right(2, true, false);
}

#[test]
fn test_skewed_left_join_right_no_checkpoints() {
    test_skewed_join_right(1, false, true);
}

#[test]
fn test_skewed_left_join_right_no_checkpoints_multihost() {
    test_skewed_join_right(2, false, true);
}

#[test]
fn test_skewed_left_join_right_checkpoints() {
    test_skewed_join_right(1, true, true);
}

#[test]
fn test_skewed_left_join_right_checkpoints_multihost() {
    test_skewed_join_right(2, true, true);
}

/// Ten skewed steps for [test_circular_dependency].
fn circular_dependency_inputs() -> Vec<(TestBatch, TestBatch)> {
    (0..10)
        .map(|_| {
            (
                generate_skewed_batch(4, 100, 5),
                generate_skewed_batch(4, 10, 5),
            )
        })
        .collect()
}

#[test]
fn test_circular_dependency_small_step() {
    test_circular_dependency(4, 1, false, false, circular_dependency_inputs());
}

#[test]
fn test_circular_dependency_small_step_multihost() {
    test_circular_dependency(4, 2, false, false, circular_dependency_inputs());
}

#[test]
fn test_circular_dependency_big_step() {
    test_circular_dependency(4, 1, true, false, circular_dependency_inputs());
}

#[test]
fn test_circular_dependency_big_step_multihost() {
    test_circular_dependency(4, 2, true, false, circular_dependency_inputs());
}

#[test]
fn test_circular_dependency_checkpoints() {
    test_circular_dependency(4, 1, false, true, circular_dependency_inputs());
}

#[test]
fn test_circular_dependency_checkpoints_multihost() {
    test_circular_dependency(4, 2, false, true, circular_dependency_inputs());
}

proptest! {
    // Reduce the number of random tests; otherwise they take too long to run.
    #![proptest_config(ProptestConfig::with_cases(16))]

    #[test]
    fn proptest_join_with_balancer_small_step(inputs in generate_join_test_data(10, 10, 10, 30)) {
        test_join_with_balancer(4, 1, false, false, inputs, false, true);
    }

    #[test]
    fn proptest_join_with_balancer_small_step_multihost(inputs in generate_join_test_data(10, 10, 10, 30)) {
        test_join_with_balancer(4, 2, false, false, inputs, false, true);
    }

    #[test]
    fn proptest_join_with_balancer_big_step(inputs in generate_join_test_data(10, 10, 10, 40)) {
        test_join_with_balancer(4, 1, true, false, inputs, false, true);
    }

    #[test]
    fn proptest_join_with_balancer_big_step_multihost(inputs in generate_join_test_data(10, 10, 10, 40)) {
        test_join_with_balancer(4, 2, true, false, inputs, false, true);
    }

    #[test]
    fn proptest_left_join_with_balancer_small_step(inputs in generate_left_join_test_data(200, 10, 10, 30)) {
        test_join_with_balancer(4, 1, false, false, inputs, true, true);
    }

    #[test]
    fn proptest_left_join_with_balancer_small_step_multihost(inputs in generate_left_join_test_data(200, 10, 10, 30)) {
        test_join_with_balancer(4, 2, false, false, inputs, true, true);
    }

    #[test]
    fn proptest_left_join_with_balancer_big_step(inputs in generate_left_join_test_data(200, 10, 10, 40)) {
        test_join_with_balancer(4, 1, true, false, inputs, true, true);
    }

    #[test]
    fn proptest_left_join_with_balancer_big_step_multihost(inputs in generate_left_join_test_data(200, 10, 10, 40)) {
        test_join_with_balancer(4, 2, true, false, inputs, true, true);
    }

}
