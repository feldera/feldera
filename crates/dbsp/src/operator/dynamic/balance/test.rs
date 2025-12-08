use crate::{
    circuit::{CircuitConfig, GlobalNodeId},
    dynamic::Data,
    operator::dynamic::balance::{BalancerHint, Policy},
    typed_batch::{IndexedZSetReader, Spine, SpineSnapshot},
    utils::Tup2,
    IndexedZSetHandle, OrdIndexedZSet, OutputHandle, RootCircuit, Runtime,
};
use anyhow::Result as AnyResult;

fn balance_batch<B>(
    batch: &B,
    policy: Policy,
    num_shards: usize,
) -> Vec<OrdIndexedZSet<B::Key, B::Val>>
where
    B: IndexedZSetReader,
{
    let mut tuples = vec![vec![]; num_shards];

    for (key, val, weight) in batch.iter() {
        let shards = match policy {
            Policy::Shard => vec![key.default_hash() % num_shards as u64],
            Policy::Broadcast => (0..num_shards as u64).collect(),
            Policy::Balance => vec![val.default_hash() % num_shards as u64],
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

    let (balanced_stream, balanced_trace) = input.accumulate_trace_balanced();
    let balanced_stream_output = balanced_stream.accumulate_output();
    let balanced_trace_output = balanced_trace
        .apply(|trace| trace.ro_snapshot().consolidate())
        .output();

    Ok((
        input_handle,
        input_node_id,
        balanced_stream_output,
        balanced_trace_output,
    ))
}

/// Test Policy::Shard
fn test_accumulate_trace_with_balancer(workers: usize, transaction: bool, policy: Policy) {
    let (mut circuit, (input_handle, input_node_id, output_delta, output_trace)) =
        Runtime::init_circuit(
            CircuitConfig::from(workers).with_splitter_chunk_size_records(2),
            accumulate_trace_with_balancer_test_circuit,
        )
        .unwrap();

    circuit
        .set_balancer_hint(&input_node_id, BalancerHint::Policy(Some(policy)))
        .unwrap();

    let mut all_tuples = vec![];
    for step in 0..10 {
        println!("step: {}", step);

        let mut tuples = vec![];
        for key in 0..10 {
            input_handle.push(key, (step, 1));
            tuples.push(Tup2(Tup2(key, step), 1));
            all_tuples.push(Tup2(Tup2(key, step), 1));
        }

        let input_delta = OrdIndexedZSet::from_tuples((), tuples);
        let expected_output_delta = balance_batch(&input_delta, policy, workers);

        let input_trace = OrdIndexedZSet::from_tuples((), all_tuples.clone().into_iter().collect());
        let expected_output_trace = balance_batch(&input_trace, policy, workers);

        circuit.transaction().unwrap();

        let output_delta: Vec<_> = (0..workers)
            .map(|worker| output_delta.take_from_worker(worker).unwrap().consolidate())
            .collect();

        let output_trace: Vec<_> = (0..workers)
            .map(|worker| output_trace.take_from_worker(worker).unwrap())
            .collect();

        assert_eq!(output_delta, expected_output_delta);
        assert_eq!(output_trace, expected_output_trace);
    }
}

#[test]
fn test_accumulate_trace_with_balancer_shard_small_step() {
    test_accumulate_trace_with_balancer(4, false, Policy::Shard);
}

#[test]
fn test_accumulate_trace_with_balancer_bcast_small_step() {
    test_accumulate_trace_with_balancer(4, false, Policy::Broadcast);
}

#[test]
fn test_accumulate_trace_with_balancer_balance_small_step() {
    test_accumulate_trace_with_balancer(4, false, Policy::Balance);
}
