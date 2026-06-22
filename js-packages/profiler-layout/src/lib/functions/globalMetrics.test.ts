import { describe, expect, it } from 'vitest'
import { buildGlobalMetrics, type GlobalMetrics } from './globalMetrics.js'

/** A fully-populated stats payload, including current gauges that the tile must drop. */
function fullMetrics(): GlobalMetrics & Record<string, number> {
  return {
    total_input_records: 1500,
    total_input_bytes: 2048,
    total_processed_records: 1400,
    total_processed_bytes: 1024,
    total_completed_records: 1300,
    total_initiated_steps: 7,
    total_completed_steps: 6,
    cpu_msecs: 5000,
    runtime_elapsed_msecs: 12_000,
    uptime_msecs: 90_000,
    // Current gauges — must be excluded from the overview tile.
    rss_bytes: 999_999,
    storage_bytes: 888_888,
    buffered_input_records: 42,
    buffered_input_bytes: 64
  }
}

describe('buildGlobalMetrics', () => {
  it('returns an empty list when no stats are present', () => {
    expect(buildGlobalMetrics(undefined)).toEqual([])
    expect(buildGlobalMetrics({})).toEqual([])
  })

  it('excludes current gauges such as rss_bytes and storage_bytes', () => {
    const keys = buildGlobalMetrics(fullMetrics()).map((e) => e.key)
    expect(keys).not.toContain('rss_bytes')
    expect(keys).not.toContain('storage_bytes')
    expect(keys).not.toContain('buffered_input_records')
    expect(keys).not.toContain('buffered_input_bytes')
  })

  it('emits the cumulative metrics in a stable presentation order', () => {
    const keys = buildGlobalMetrics(fullMetrics()).map((e) => e.key)
    expect(keys).toEqual([
      'total_input_records',
      'total_input_bytes',
      'total_processed_records',
      'total_processed_bytes',
      'total_completed_records',
      'total_initiated_steps',
      'total_completed_steps',
      'cpu_msecs',
      'runtime_elapsed_msecs',
      'uptime_msecs'
    ])
  })

  it('formats each value with the matching unit', () => {
    const byKey = new Map(buildGlobalMetrics(fullMetrics()).map((e) => [e.key, e.value.toString()]))
    expect(byKey.get('total_input_records')).toBe('1.5K') // count, base 1000
    expect(byKey.get('total_input_bytes')).toBe('2KiB') // bytes, base 1024
    expect(byKey.get('cpu_msecs')).toBe('5s') // milliseconds rendered as seconds
  })

  it('skips fields that are absent or not finite numbers', () => {
    const entries = buildGlobalMetrics({
      total_input_records: 10,
      total_input_bytes: Number.NaN,
      total_processed_records: Number.POSITIVE_INFINITY,
      // A stray non-numeric value (e.g. from a hand-edited stats file) is ignored, not rendered.
      cpu_msecs: '5000' as unknown as number
    })
    expect(entries.map((e) => e.key)).toEqual(['total_input_records'])
  })
})
