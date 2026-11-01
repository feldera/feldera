import { describe, expect, it } from 'vitest'
import { levelMetrics, metricToSelect } from './metricLevel'

const option = (id: string) => ({ id, label: id })
const metrics = [
  'input_batches_stats.record_count',
  'runtime_percent',
  'runtime_seconds',
  'spine_storage_size_bytes'
].map(option)

describe('levelMetrics', () => {
  it('lists only the basic metrics at the basic level', () => {
    expect(levelMetrics(metrics, 'basic').map((m) => m.id)).toEqual([
      'input_batches_stats.record_count',
      'runtime_seconds',
      'spine_storage_size_bytes'
    ])
  })

  it('lists the other metrics too at the standard level', () => {
    expect(levelMetrics(metrics, 'standard')).toEqual(metrics)
  })
})

describe('metricToSelect', () => {
  it('keeps a listed metric', () => {
    const listed = levelMetrics(metrics, 'basic')
    expect(metricToSelect('spine_storage_size_bytes', listed)).toBe('spine_storage_size_bytes')
  })

  it('replaces a hidden metric with the runtime', () => {
    // The default metric, runtime_percent, is not a basic metric.
    expect(metricToSelect('runtime_percent', levelMetrics(metrics, 'basic'))).toBe(
      'runtime_seconds'
    )
  })

  it('replaces a hidden metric with the first listed one when the runtime is not listed', () => {
    const listed = [option('spine_storage_size_bytes'), option('input_batches_stats.record_count')]
    expect(metricToSelect('runtime_percent', listed)).toBe('spine_storage_size_bytes')
  })
})
