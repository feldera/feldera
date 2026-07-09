import { describe, expect, it } from 'vitest'
import { CARD_KINDS, isCardKind, type MetricKind, metricKind, splitBaseId } from './kind.js'

describe('metricKind', () => {
  // Exhaustive expected classification for every metric_id defined in CIRCUIT_METRICS
  // (crates/dbsp/src/circuit/metadata.rs) plus the `persistent_id` node attribute. Keep in sync
  // when metrics are added on the Rust side; a new metric that lands here as K1 by default
  // should be moved to the kind it visualizes best. See ./README.md.
  const EXPECTED: Record<MetricKind, string[]> = {
    K1: [
      'accumulator_records_to_repartition_count',
      'allocated_memory_bytes',
      'bloom_filter_bits_per_key',
      'bloom_filter_hits_count',
      'bloom_filter_misses_count',
      'bloom_filter_size_bytes',
      'circuit_idle_time_seconds',
      'circuit_runtime_elapsed_seconds',
      'circuit_runtime_seconds',
      'circuit_wait_time_seconds',
      'computed_output_records_count',
      'exchange_deserialization_time_seconds',
      'exchange_deserialized_bytes',
      'exchange_serialization_time_seconds',
      'exchange_serialized_bytes',
      'exchange_wait_time_seconds',
      'inprogress_rebalancing_time_seconds',
      'input_integral_records_count',
      'input_records_count',
      'integral_records_to_repartition_count',
      'invocations_count',
      'left_input_records_count',
      'local_shard_records_count',
      'loose_batches_count',
      'loose_memory_records_count',
      'loose_storage_records_count',
      'memory_allocations_count',
      'merge_backpressure_wait_time_seconds',
      'merging_batches_count',
      'merging_memory_records_count',
      'merging_size_bytes',
      'merging_storage_records_count',
      'negative_weight_count',
      'range_filter_hits_count',
      'range_filter_misses_count',
      'range_filter_size_bytes',
      'rebalancings_count',
      'right_input_integral_records_count',
      'roaring_filter_hits_count',
      'roaring_filter_misses_count',
      'roaring_filter_size_bytes',
      'runtime_seconds',
      'shared_memory_bytes',
      'spine_batches_count',
      'spine_count',
      'spine_storage_size_bytes',
      'state_records_count',
      'steps_count',
      'total_rebalancing_time_seconds',
      'used_memory_bytes'
    ],
    K2: [
      'background_cache_hit_rate_percent',
      'bloom_filter_hit_rate_percent',
      'foreground_cache_hit_rate_percent',
      'merge_reduction_percent',
      'nonblocking_percent',
      'output_redundancy_percent',
      'range_filter_hit_rate_percent',
      'roaring_filter_hit_rate_percent',
      'runtime_percent'
    ],
    K3: ['background_cache_occupancy', 'foreground_cache_occupancy'],
    K4: ['balancer_policy', 'retainment_bounds', 'persistent_id'],
    K5: ['rebalancing_in_progress_bool'],
    K6: ['compaction_state'],
    K7: [
      'input_batches_stats',
      'left_input_batches_stats',
      'output_batches_stats',
      'prefix_batches_stats',
      'right_input_batches_stats'
    ],
    K8: [
      'background_cache_hits',
      'background_cache_misses',
      'foreground_cache_hits',
      'foreground_cache_misses'
    ],
    K9: ['completed_merges'],
    K10: ['key_distribution'],
    K11: ['size_distribution']
  }

  for (const [kind, ids] of Object.entries(EXPECTED) as Array<[MetricKind, string[]]>) {
    it.each(ids)(`classifies %s as ${kind}`, (metric) => {
      expect(metricKind(metric)).toBe(kind)
    })
  }

  it('falls back to K1 for unknown suffixes', () => {
    expect(metricKind('some_new_metric_widget')).toBe('K1')
  })
})

describe('splitBaseId', () => {
  it('returns the whole id and empty suffix when there is no dot', () => {
    expect(splitBaseId('used_memory_bytes')).toEqual({
      baseId: 'used_memory_bytes',
      suffix: ''
    })
  })

  it('splits a composite sub-row at the first dot', () => {
    expect(splitBaseId('output_batches_stats.avg_size')).toEqual({
      baseId: 'output_batches_stats',
      suffix: '.avg_size'
    })
  })

  it('keeps labels and trailing sub-fields together in the suffix', () => {
    expect(splitBaseId('completed_merges.slot:0.steps')).toEqual({
      baseId: 'completed_merges',
      suffix: '.slot:0.steps'
    })
  })
})

describe('isCardKind', () => {
  it('routes every non-bar kind to its own card', () => {
    for (const k of [
      'K2',
      'K3',
      'K4',
      'K5',
      'K6',
      'K7',
      'K8',
      'K9',
      'K10',
      'K11'
    ] as MetricKind[]) {
      expect(isCardKind(k)).toBe(true)
    }
  })

  it('keeps K1 (bar chart) in the distribution grid', () => {
    expect(isCardKind('K1')).toBe(false)
    expect(CARD_KINDS.has('K1')).toBe(false)
  })
})
