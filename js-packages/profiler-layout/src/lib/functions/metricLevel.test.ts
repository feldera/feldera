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
    expect(levelMetrics(metrics, false).map((m) => m.id)).toEqual([
      'input_batches_stats.record_count',
      'runtime_seconds',
      'spine_storage_size_bytes'
    ])
  })

  it('lists the other metrics too at the advanced level', () => {
    expect(levelMetrics(metrics, true)).toEqual(metrics)
  })
})

describe('metricToSelect', () => {
  it('keeps a listed metric', () => {
    const listed = levelMetrics(metrics, false)
    expect(metricToSelect('spine_storage_size_bytes', listed)).toBe('spine_storage_size_bytes')
  })

  it('replaces a hidden metric with the runtime', () => {
    // The default metric, runtime_percent, is not a basic metric.
    expect(metricToSelect('runtime_percent', levelMetrics(metrics, false))).toBe('runtime_seconds')
  })

  it('replaces a hidden metric with the first listed one when the runtime is not listed', () => {
    const listed = [option('spine_storage_size_bytes'), option('input_batches_stats.record_count')]
    expect(metricToSelect('runtime_percent', listed)).toBe('spine_storage_size_bytes')
  })
})
