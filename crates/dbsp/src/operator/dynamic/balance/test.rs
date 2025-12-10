use crate::{
    circuit::{CircuitConfig, GlobalNodeId},
    dynamic::Data,
    operator::dynamic::balance::{BalancerHint, Policy},
    typed_batch::{IndexedZSetReader, SpineSnapshot, TypedBatch},
    utils::Tup2,
    IndexedZSetHandle, OrdIndexedZSet, OutputHandle, RootCircuit, Runtime, ZWeight,
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

fn test_accumulate_trace_with_balancer(
    workers: usize,
    transaction: bool,
    batches: Vec<(Policy, Vec<Tup2<u64, Tup2<u64, ZWeight>>>)>,
) {
    let (mut circuit, (input_handle, input_node_id, output_delta, output_trace)) =
        Runtime::init_circuit(
            CircuitConfig::from(workers).with_splitter_chunk_size_records(2),
            accumulate_trace_with_balancer_test_circuit,
        )
        .unwrap();

    let mut all_tuples: Vec<Tup2<Tup2<u64, u64>, ZWeight>> = vec![];

    if transaction {
        circuit.start_transaction().unwrap();
    }

    let mut previous_policy = Policy::Shard;

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

fn simple_workload(policy: Policy) -> Vec<(Policy, Vec<Tup2<u64, Tup2<u64, ZWeight>>>)> {
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

fn round_robin_workload() -> Vec<(Policy, Vec<Tup2<u64, Tup2<u64, ZWeight>>>)> {
    let mut batches = vec![];
    for step in 0..20 {
        let mut tuples = vec![];
        for key in 0..20 {
            tuples.push(Tup2(key, Tup2(step, 1)));
        }
        batches.push((Policy::try_from((step % 3) as u8).unwrap(), tuples));
    }
    batches
}

#[test]
fn test_accumulate_trace_with_balancer_shard_small_step() {
    test_accumulate_trace_with_balancer(4, false, simple_workload(Policy::Shard));
}

#[test]
fn test_accumulate_trace_with_balancer_bcast_small_step() {
    test_accumulate_trace_with_balancer(4, false, simple_workload(Policy::Broadcast));
}

#[test]
fn test_accumulate_trace_with_balancer_balance_small_step() {
    test_accumulate_trace_with_balancer(4, false, simple_workload(Policy::Balance));
}

#[test]
fn test_accumulate_trace_with_balancer_shard_big_step() {
    test_accumulate_trace_with_balancer(4, true, simple_workload(Policy::Shard));
}

#[test]
fn test_accumulate_trace_with_balancer_bcast_big_step() {
    test_accumulate_trace_with_balancer(4, true, simple_workload(Policy::Broadcast));
}

#[test]
fn test_accumulate_trace_with_balancer_balance_big_step() {
    test_accumulate_trace_with_balancer(4, true, simple_workload(Policy::Balance));
}

#[test]
fn test_accumulate_trace_with_balancer_round_robin_small_step() {
    test_accumulate_trace_with_balancer(4, false, round_robin_workload());
}

#[test]
fn test_accumulate_trace_with_balancer_round_robin_big_step() {
    test_accumulate_trace_with_balancer(4, true, round_robin_workload());
}
