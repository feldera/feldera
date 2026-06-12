import { BigNumber } from 'bignumber.js'
import { describe, expect, it } from 'vitest'
import { timeSeriesAxisMax } from './pipelineMetrics'
import type { TimeSeriesEntry } from '$lib/types/pipelineManager'

const sampleAt = (timeMs: number): TimeSeriesEntry => ({
  t: new BigNumber(timeMs),
  r: new BigNumber(0),
  m: new BigNumber(0),
  s: new BigNumber(0)
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
