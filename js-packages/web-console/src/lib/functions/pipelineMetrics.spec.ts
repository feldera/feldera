import { describe, expect, it } from 'vitest'
import type { TimeSeriesEntry } from '$lib/types/pipelineManager'
import { calcPipelineLatency, multihostMemoryLimitMb, timeSeriesAxisMax } from './pipelineMetrics'

const sampleAt = (timeMs: number): TimeSeriesEntry => ({
  t: timeMs,
  r: 0,
  m: 0,
  s: 0
})

describe('timeSeriesAxisMax', () => {
  it('anchors to the newest sample regardless of the client clock', () => {
    const metrics = [sampleAt(1000), sampleAt(2000), sampleAt(3000)]
    // A client clock that disagrees with the server must not influence the result.
    expect(timeSeriesAxisMax(metrics, () => 9999)).toBe(3000)
  })

  it('falls back to the supplied time source when there are no samples', () => {
    expect(timeSeriesAxisMax([], () => 4242)).toBe(4242)
  })
})

describe('multihostMemoryLimitMb', () => {
  it('returns the per-host limit unchanged for a single host', () => {
    expect(multihostMemoryLimitMb(2048, 1)).toBe(2048)
  })

  it('scales the per-host limit by the number of hosts in a multihost deployment', () => {
    // The reported memory metric sums RSS across all hosts, so the limit line
    // must reflect the aggregate ceiling, not the per-host limit.
    expect(multihostMemoryLimitMb(2048, 3)).toBe(3 * 2048)
  })

  it('treats a missing or non-positive host count as a single host', () => {
    expect(multihostMemoryLimitMb(2048, undefined)).toBe(2048)
    expect(multihostMemoryLimitMb(2048, null)).toBe(2048)
    expect(multihostMemoryLimitMb(2048, 0)).toBe(2048)
  })

  it('returns undefined when no memory limit is configured', () => {
    expect(multihostMemoryLimitMb(undefined, 3)).toBeUndefined()
    expect(multihostMemoryLimitMb(null, 3)).toBeUndefined()
    expect(multihostMemoryLimitMb(0, 3)).toBeUndefined()
  })
})

describe('calcPipelineLatency', () => {
  const sample = (t: number, l: Partial<TimeSeriesEntry>): TimeSeriesEntry => ({
    t,
    r: 0,
    m: 0,
    s: 0,
    ...l
  })

  it('reports no data when latency fields are absent', () => {
    const result = calcPipelineLatency([sample(1000, {}), sample(2000, {})])
    expect(result.hasData).toBe(false)
    expect(result.series.processingP99).toEqual([])
    expect(result.series.completionP50).toEqual([])
  })

  it('splits the four series and pairs each value with its timestamp', () => {
    const metrics = [
      sample(1000, { pp50: 100, pp99: 500, cp50: 200, cp99: 900 }),
      sample(2000, { pp50: 150, pp99: 600, cp50: 250, cp99: 950 })
    ]
    const result = calcPipelineLatency(metrics)
    expect(result.hasData).toBe(true)
    expect(result.series.processingP50).toEqual([
      [1000, 100],
      [2000, 150]
    ])
    expect(result.series.completionP99).toEqual([
      [1000, 900],
      [2000, 950]
    ])
  })

  it('skips samples missing a given field, keeping series independent', () => {
    const metrics = [
      sample(1000, { pp50: 100 }),
      sample(2000, { pp50: 150, cp99: 900 }),
      sample(3000, { cp99: 950 })
    ]
    const result = calcPipelineLatency(metrics)
    expect(result.series.processingP50).toEqual([
      [1000, 100],
      [2000, 150]
    ])
    expect(result.series.completionP99).toEqual([
      [2000, 900],
      [3000, 950]
    ])
  })

  it('scales the y-axis to the p99 lines, above the p50 values', () => {
    const result = calcPipelineLatency([sample(1000, { pp50: 10, pp99: 800, cp99: 1200 })])
    // yMax must exceed the largest p99 (1200) so the line is not clipped.
    expect(result.yMin).toBe(0)
    expect(result.yMax).toBeGreaterThanOrEqual(1200)
  })

  it('tightens the baseline axis max to the p50 lines only', () => {
    const result = calcPipelineLatency([sample(1000, { pp50: 10, pp99: 800, cp99: 1200 })])
    // With p99 hidden the axis should scale to the p50 value (10), well below yMax.
    expect(result.yMaxBaseline).toBeGreaterThanOrEqual(10)
    expect(result.yMaxBaseline).toBeLessThan(result.yMax)
  })
})
