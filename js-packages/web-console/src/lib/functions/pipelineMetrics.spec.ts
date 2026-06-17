import { describe, expect, it } from 'vitest'
import type { TimeSeriesEntry } from '$lib/types/pipelineManager'
import { multihostMemoryLimitMb, timeSeriesAxisMax } from './pipelineMetrics'

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
