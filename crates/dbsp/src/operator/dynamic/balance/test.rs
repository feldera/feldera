use feldera_types::config::{StorageCacheConfig, StorageConfig, StorageOptions};
use proptest::collection::vec;
use proptest::prelude::*;
use std::hash::Hasher;
use tempfile::{TempDir, tempdir};

use crate::{
    Circuit, IndexedZSetHandle, OrdIndexedZSet, OrdZSet, OutputHandle, RootCircuit, Runtime,
    ZWeight,
    circuit::{CircuitConfig, CircuitStorageConfig, GlobalNodeId},
    default_hasher,
    dynamic::{Data, DowncastTrait as _},
    operator::{
        dynamic::balance::{BalancerHint, PartitioningPolicy},
        output::AccumulateOutput,
    },
    trace::{BatchReader as _, Cursor as _},
    typed_batch::{BatchReader, IndexedZSetReader, SpineSnapshot, TypedBatch},
    utils::{Tup2, Tup3, test::init_test_logger},
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

fn accumulate_trace_with_balancer_test_circuit(
    circuit: &mut RootCircuit,
) -> AnyResult<(
    IndexedZSetHandle<u64, u64>,
    GlobalNodeId,
    OutputHandle<SpineSnapshot<OrdIndexedZSet<u64, u64>>>,
    OutputHandle<OrdIndexedZSet<u64, u64>>,
)> {
    let (input, input_handle) = circuit.add_input_indexed_zset::<u64, u64>();
    let input_node_id = input.origin_node_id().clone();

    let (balanced_accumulator, balanced_trace) = input.accumulate_trace_balanced();

    let (balanced_stream_output, output_handle) =
        AccumulateOutput::<OrdIndexedZSet<u64, u64>>::new();

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
    GlobalNodeId,
    GlobalNodeId,
    OutputHandle<OrdZSet<Tup3<u64, u64, Option<u64>>>>,
);

fn join_with_balancer_test_circuit(circuit: &mut RootCircuit) -> AnyResult<JoinTestCircuitResult> {
    let (left_input, left_input_handle) = circuit.add_input_indexed_zset::<u64, u64>();
    let (right_input, right_input_handle) = circuit.add_input_indexed_zset::<u64, u64>();
    let left_input_node_id = left_input.origin_node_id().clone();
    let right_input_node_id = right_input.origin_node_id().clone();

    // let balanced_stream_output = balanced_stream.accumulate_output();
    let join_output_handle = left_input
        .join_balanced_inner(&right_input, |key, v1, v2| Tup3(*key, *v1, Some(*v2)))
        .accumulate_integrate_trace()
        .apply(|trace| trace.ro_snapshot().consolidate())
        .output();

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
    GlobalNodeId,
    GlobalNodeId,
    OutputHandle<OrdZSet<Tup3<u64, u64, Option<u64>>>>,
)> {
    let (left_input, left_input_handle) = circuit.add_input_indexed_zset::<u64, u64>();
    let (right_input, right_input_handle) = circuit.add_input_indexed_zset::<u64, u64>();

    let right_input = right_input.map_index(|(k, v)| (k.clone(), Some(v.clone())));

    let left_input_node_id = left_input.origin_node_id().clone();
    let right_input_node_id = right_input.origin_node_id().clone();

    // let balanced_stream_output = balanced_stream.accumulate_output();
    let join_output_handle = left_input
        .left_join_balanced_inner(&right_input, |key, v1, v2| Tup3(*key, *v1, *v2))
        .accumulate_integrate_trace()
        .apply(|trace| trace.ro_snapshot().consolidate())
        .output();

    Ok((
        left_input_handle,
        right_input_handle,
        left_input_node_id,
        right_input_node_id,
        join_output_handle,
    ))
}

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
) -> AnyResult<(
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
)> {
    let (left_input, left_input_handle) = circuit.add_input_indexed_zset::<u64, u64>();
    let (right_input, right_input_handle) = circuit.add_input_indexed_zset::<u64, u64>();

    let s2 = left_input.map_index(|(k, v)| (k.clone(), v.clone()));
    let s3 = right_input.map_index(|(k, v)| (k.clone(), v.clone()));
    let s5 = left_input.map_index(|(k, v)| (k.clone(), v.clone()));
    let s6 = right_input.map_index(|(k, v)| (k.clone(), v.clone()));

    let s1 = s5.join_index_balanced_inner(&s6, |key, v1, v2| Some((*key, Tup2(*v1, *v2))));
    let s4 = s2.join_index_balanced_inner(&s3, |key, v1, v2| Some((*key, Tup2(*v1, *v2))));
    let s7 = s4.join_index_balanced_inner(&s5, |key, Tup2(v1, v2), v3| {
        Some((*key, Tup3(*v1, *v2, *v3)))
    });
    let s8 = s2.join_index_balanced_inner(&s1, |key, v1, Tup2(v2, v3)| {
        Some((*key, Tup3(*v1, *v2, *v3)))
    });

    let output1 = s1.accumulate_output();
    let output4 = s4.accumulate_output();
    let output7 = s7.accumulate_output();
    let output8 = s8.accumulate_output();

    // Reference outputs computed using regular joins.
    let s1_ref = s5.join_index(&s6, |key, v1, v2| Some((*key, Tup2(*v1, *v2))));
    let s4_ref = s2.join_index(&s3, |key, v1, v2| Some((*key, Tup2(*v1, *v2))));
    let s7_ref = s4.join_index(&s5, |key, Tup2(v1, v2), v3| {
        Some((*key, Tup3(*v1, *v2, *v3)))
    });
    let s8_ref = s2.join_index(&s1, |key, v1, Tup2(v2, v3)| {
        Some((*key, Tup3(*v1, *v2, *v3)))
    });

    let output1_ref = s1_ref.accumulate_output();
    let output4_ref = s4_ref.accumulate_output();
    let output7_ref = s7_ref.accumulate_output();
    let output8_ref = s8_ref.accumulate_output();

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

fn test_accumulate_trace_with_balancer(
    workers: usize,
    transaction: bool,
    batches: Vec<(PartitioningPolicy, Vec<Tup2<u64, Tup2<u64, ZWeight>>>)>,
) {
    let (mut circuit, (input_handle, input_node_id, output_delta, output_trace)) =
        Runtime::init_circuit(
            CircuitConfig::from(workers)
                .with_splitter_chunk_size_records(2)
                .with_balancer_min_absolute_improvement_threshold(0),
            accumulate_trace_with_balancer_test_circuit,
        )
        .unwrap();

    let mut all_tuples: Vec<Tup2<Tup2<u64, u64>, ZWeight>> = vec![];

    if transaction {
        circuit.start_transaction().unwrap();
    }

    let mut previous_policy = PartitioningPolicy::Shard;

    for (step, (policy, batch)) in batches.iter().enumerate() {
        println!("step: {}", step);

        circuit
            .set_balancer_hint(&input_node_id, BalancerHint::Policy(Some(*policy)))
            .unwrap();

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
            circuit.step().unwrap();
        } else {
            circuit.transaction().unwrap();
        }

        if !transaction {
            let output_delta: Vec<_> = (0..workers)
                .map(|worker| output_delta.take_from_worker(worker).unwrap().consolidate())
                .collect();

            let output_trace: Vec<_> = (0..workers)
                .map(|worker| output_trace.take_from_worker(worker).unwrap())
                .collect();

            assert_eq!(output_delta, expected_output_delta);
            assert_eq!(output_trace, expected_output_trace);
        }

        previous_policy = *policy;
    }

    if transaction {
        circuit.commit_transaction().unwrap();
        let output_delta: Vec<_> = (0..workers)
            .map(|worker| output_delta.take_from_worker(worker).unwrap().consolidate())
            .collect();

        let output_trace: Vec<_> = (0..workers)
            .map(|worker| output_trace.take_from_worker(worker).unwrap())
            .collect();

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

pub(crate) fn mkconfig(workers: usize) -> (TempDir, CircuitConfig) {
    let temp = tempdir().expect("Can't create temp dir for storage");

    let storage = CircuitStorageConfig::for_config(
        StorageConfig {
            path: temp.path().to_string_lossy().into_owned(),
            cache: StorageCacheConfig::default(),
        },
        StorageOptions::default(),
    )
    .unwrap();

    let cconf = CircuitConfig::from(workers)
        .with_splitter_chunk_size_records(2)
        .with_balancer_min_absolute_improvement_threshold(0)
        .with_storage(storage);
    (temp, cconf)
}

fn test_join_with_balancer(
    workers: usize,
    transaction: bool,
    checkpoints: bool,
    inputs: Vec<JoinTestStep>,
    left_join: bool,
) {
    init_test_logger();

    // Can't make checkpoints mid-transaction.
    assert!(!(checkpoints && transaction));

    // The last step must have a specific policy to check.
    assert!(inputs.last().unwrap().expected_left_policy.is_some());
    assert!(inputs.last().unwrap().expected_right_policy.is_some());

    let (_temp, mut cconf) = mkconfig(workers);

    let constructor = if left_join {
        left_join_with_balancer_test_circuit
    } else {
        join_with_balancer_test_circuit
    };

    let (
        mut circuit,
        (
            mut left_input_handle,
            mut right_input_handle,
            mut left_input_node_id,
            mut right_input_node_id,
            mut output_integral_handle,
        ),
    ) = Runtime::init_circuit(cconf.clone(), constructor).unwrap();

    let mut all_left_tuples: Vec<Tup2<Tup2<u64, u64>, ZWeight>> = vec![];
    let mut all_right_tuples: Vec<Tup2<Tup2<u64, u64>, ZWeight>> = vec![];

    if transaction {
        circuit.start_transaction().unwrap();
    }

    for (
        step,
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
        println!(
            "step: {} (left_policy_hint: {:?}, right_policy_hint: {:?})",
            step, left_policy_hint, right_policy_hint
        );

        circuit
            .set_balancer_hints(vec![
                (left_input_node_id.clone(), BalancerHint::Policy(None)),
                (right_input_node_id.clone(), BalancerHint::Policy(None)),
            ])
            .unwrap();

        if let Some(left_policy_hint) = left_policy_hint {
            circuit
                .set_balancer_hint(&left_input_node_id, BalancerHint::Policy(*left_policy_hint))
                .unwrap();
        }

        if let Some(right_policy_hint) = right_policy_hint {
            circuit
                .set_balancer_hint(
                    &right_input_node_id,
                    BalancerHint::Policy(*right_policy_hint),
                )
                .unwrap();
        }

        // println!("insertions: {:?}", insertions);

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
            circuit.step().unwrap();
        } else {
            circuit.transaction().unwrap();
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

            let output_trace: Vec<_> = (0..workers)
                .map(|worker| output_integral_handle.take_from_worker(worker).unwrap())
                .collect();

            let current_policies = circuit.get_current_balancer_policy().unwrap();
            let current_left_policy = current_policies.get(&left_input_node_id).unwrap();
            let current_right_policy = current_policies.get(&right_input_node_id).unwrap();

            assert_eq!(current_left_policy, &expected_left_policy.unwrap());
            assert_eq!(current_right_policy, &expected_right_policy.unwrap());
            assert_eq!(output_trace, expected_output);

            if checkpoints {
                // checkpoint and stop the circuit.
                let cpm = circuit.checkpoint().run().expect("commit shouldn't fail");
                circuit.kill().unwrap();

                // start from the checkpoint
                cconf.storage.as_mut().unwrap().init_checkpoint = Some(cpm.uuid);

                (
                    circuit,
                    (
                        left_input_handle,
                        right_input_handle,
                        left_input_node_id,
                        right_input_node_id,
                        output_integral_handle,
                    ),
                ) = Runtime::init_circuit(cconf.clone(), constructor).unwrap();
            }
        }
    }

    if transaction {
        circuit.commit_transaction().unwrap();
        let output_trace: Vec<_> = (0..workers)
            .map(|worker| output_integral_handle.take_from_worker(worker).unwrap())
            .collect();

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

fn test_circular_dependency(
    num_workers: usize,
    transaction: bool,
    checkpoints: bool,
    inputs: Vec<(TestBatch, TestBatch)>,
) {
    init_test_logger();

    // Can't make checkpoints mid-transaction.
    assert!(!(checkpoints && transaction));

    let (_temp, mut cconf) = mkconfig(num_workers);

    let (
        mut circuit,
        (
            mut input_handle1,
            mut input_handle2,
            mut output_handle1,
            mut output_handle4,
            mut output_handle7,
            mut output_handle8,
            mut output_handle1_ref,
            mut output_handle4_ref,
            mut output_handle7_ref,
            mut output_handle8_ref,
        ),
    ) = Runtime::init_circuit(cconf.clone(), circular_dependency_test_circuit).unwrap();

    if transaction {
        circuit.start_transaction().unwrap();
    }

    for (step, (left, right)) in inputs.iter().enumerate() {
        println!("step: {step}");

        // println!("insertions: {:?}", insertions);

        for Tup2(key, Tup2(val, w)) in left.iter() {
            input_handle1.push(*key, (*val, *w));
        }
        for Tup2(key, Tup2(val, w)) in right.iter() {
            input_handle2.push(*key, (*val, *w));
        }

        if transaction {
            circuit.step().unwrap();
        } else {
            circuit.transaction().unwrap();
        }

        if !transaction {
            assert_eq!(
                output_handle1.concat().consolidate(),
                output_handle1_ref.concat().consolidate()
            );
            assert_eq!(
                output_handle4.concat().consolidate(),
                output_handle4_ref.concat().consolidate()
            );
            assert_eq!(
                output_handle7.concat().consolidate(),
                output_handle7_ref.concat().consolidate()
            );
            assert_eq!(
                output_handle8.concat().consolidate(),
                output_handle8_ref.concat().consolidate()
            );

            if checkpoints {
                // checkpoint and stop the circuit.
                let cpm = circuit.checkpoint().run().expect("commit shouldn't fail");
                circuit.kill().unwrap();

                // start from the checkpoint
                cconf.storage.as_mut().unwrap().init_checkpoint = Some(cpm.uuid);

                (
                    circuit,
                    (
                        input_handle1,
                        input_handle2,
                        output_handle1,
                        output_handle4,
                        output_handle7,
                        output_handle8,
                        output_handle1_ref,
                        output_handle4_ref,
                        output_handle7_ref,
                        output_handle8_ref,
                    ),
                ) = Runtime::init_circuit(cconf.clone(), circular_dependency_test_circuit).unwrap();
            }
        }
    }

    if transaction {
        circuit.commit_transaction().unwrap();

        assert_eq!(
            output_handle1.concat().consolidate(),
            output_handle1_ref.concat().consolidate()
        );
        assert_eq!(
            output_handle4.concat().consolidate(),
            output_handle4_ref.concat().consolidate()
        );
        assert_eq!(
            output_handle7.concat().consolidate(),
            output_handle7_ref.concat().consolidate()
        );
        assert_eq!(
            output_handle8.concat().consolidate(),
            output_handle8_ref.concat().consolidate()
        );
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
        false,
        simple_integrate_workload(PartitioningPolicy::Shard),
    );
}

#[test]
fn test_accumulate_trace_with_balancer_bcast_small_step() {
    test_accumulate_trace_with_balancer(
        4,
        false,
        simple_integrate_workload(PartitioningPolicy::Broadcast),
    );
}

#[test]
fn test_accumulate_trace_with_balancer_balance_small_step() {
    test_accumulate_trace_with_balancer(
        4,
        false,
        simple_integrate_workload(PartitioningPolicy::Balance),
    );
}

#[test]
fn test_accumulate_trace_with_balancer_shard_big_step() {
    test_accumulate_trace_with_balancer(
        4,
        true,
        simple_integrate_workload(PartitioningPolicy::Shard),
    );
}

#[test]
fn test_accumulate_trace_with_balancer_bcast_big_step() {
    test_accumulate_trace_with_balancer(
        4,
        true,
        simple_integrate_workload(PartitioningPolicy::Broadcast),
    );
}

#[test]
fn test_accumulate_trace_with_balancer_balance_big_step() {
    test_accumulate_trace_with_balancer(
        4,
        true,
        simple_integrate_workload(PartitioningPolicy::Balance),
    );
}

#[test]
fn test_accumulate_trace_with_balancer_round_robin_small_step() {
    test_accumulate_trace_with_balancer(4, false, round_robin_integrate_workload());
}

#[test]
fn test_accumulate_trace_with_balancer_round_robin_big_step() {
    test_accumulate_trace_with_balancer(4, true, round_robin_integrate_workload());
}

/// Join a large left collection with a small right collection. Both are skewed.
/// The balancer should balance the left collection using Policy::Balance and the
/// right collection using Policy::Broadcast.
fn test_skewed_join_left(left_join: bool) {
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
            expected_left_policy: if i >= 5 {
                Some(PartitioningPolicy::Balance)
            } else {
                None
            },
            expected_right_policy: if i >= 5 {
                Some(PartitioningPolicy::Broadcast)
            } else {
                None
            },
        });
    }
    test_join_with_balancer(num_partitions, false, false, test_steps, left_join);
}

#[test]
fn test_skewed_inner_join_left() {
    test_skewed_join_left(false);
}

#[test]
fn test_skewed_left_join_no_checkpoints() {
    test_skewed_join_left(true);
}

/// Join a small left collection with a large right collection. Both are skewed.
/// The balancer should balance the left collection using Policy::Broadcast and the
/// right collection using Policy::Balance.
fn test_skewed_join_right(checkpoints: bool, left_join: bool) {
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
    test_join_with_balancer(num_partitions, false, checkpoints, test_steps, left_join);
}

#[test]
fn test_skewed_join_right_no_checkpoints() {
    test_skewed_join_right(false, false);
}

#[test]
fn test_skewed_join_right_checkpoints() {
    test_skewed_join_right(true, false);
}

#[test]
fn test_skewed_left_join_right_no_checkpoints() {
    test_skewed_join_right(false, true);
}

#[test]
fn test_skewed_left_join_right_checkpoints() {
    test_skewed_join_right(true, true);
}

#[test]
fn test_circular_dependency_small_step() {
    let mut inputs = vec![];

    for _ in 0..10 {
        let left_batch = generate_skewed_batch(4, 100, 5);
        let right_batch = generate_skewed_batch(4, 10, 5);
        inputs.push((left_batch, right_batch));
    }

    test_circular_dependency(4, false, false, inputs);
}

#[test]
fn test_circular_dependency_big_step() {
    let mut inputs = vec![];

    for _ in 0..10 {
        let left_batch = generate_skewed_batch(4, 100, 5);
        let right_batch = generate_skewed_batch(4, 10, 5);
        inputs.push((left_batch, right_batch));
    }

    test_circular_dependency(4, true, false, inputs);
}

#[test]
fn test_circular_dependency_checkpoints() {
    let mut inputs = vec![];

    for _ in 0..10 {
        let left_batch = generate_skewed_batch(4, 100, 5);
        let right_batch = generate_skewed_batch(4, 10, 5);
        inputs.push((left_batch, right_batch));
    }

    test_circular_dependency(4, false, true, inputs);
}

proptest! {
    // Reduce the number of random tests; otherwise they take too long to run.
    #![proptest_config(ProptestConfig::with_cases(16))]

    #[test]
    fn proptest_join_with_balancer_small_step(inputs in generate_join_test_data(10, 10, 10, 30)) {
        test_join_with_balancer(4, false, false, inputs, false);
    }

    #[test]
    fn proptest_join_with_balancer_big_step(inputs in generate_join_test_data(10, 10, 10, 40)) {
        test_join_with_balancer(4, true, false, inputs, false);
    }

    #[test]
    fn proptest_left_join_with_balancer_small_step(inputs in generate_left_join_test_data(200, 10, 10, 30)) {
        test_join_with_balancer(4, false, false, inputs, true);
    }

    #[test]
    fn proptest_left_join_with_balancer_big_step(inputs in generate_left_join_test_data(200, 10, 10, 40)) {
        test_join_with_balancer(4, true, false, inputs, true);
    }

}
