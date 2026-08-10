import { describe, expect, it } from 'vitest'
import {
  defaultLatencyColorSpread,
  latencyColor,
  latencyColorFraction,
  latencyColorScale
} from './latencyColor'

describe('latencyColorScale', () => {
  it('returns undefined when no connector reports a latency', () => {
    expect(latencyColorScale([])).toBeUndefined()
    expect(latencyColorScale([null, undefined])).toBeUndefined()
  })

  it('ignores connectors without samples', () => {
    expect(latencyColorScale([null, 1_000, undefined, 9_000])).toEqual({ min: 1_000, max: 9_000 })
  })

  it('ignores non-finite and negative values', () => {
    expect(latencyColorScale([NaN, -5, 1_000, 9_000])).toEqual({ min: 1_000, max: 9_000 })
  })

  it('keeps the observed max when it exceeds the spread floor', () => {
    // 9000 is 9x the minimum, well above the 3x floor.
    expect(latencyColorScale([1_000, 9_000])).toEqual({ min: 1_000, max: 9_000 })
  })

  it('raises the max to the spread floor when connectors are clustered', () => {
    // Observed max is only 10% above the min, so the floor takes over.
    expect(latencyColorScale([1_000, 1_100])).toEqual({ min: 1_000, max: 5_000 })
  })

  it('applies the floor exactly at the spread multiple', () => {
    expect(latencyColorScale([1_000, 5_000])).toEqual({ min: 1_000, max: 5_000 })
  })

  it('honors a configured spread', () => {
    expect(latencyColorScale([1_000, 1_100], 10)).toEqual({ min: 1_000, max: 10_000 })
    expect(latencyColorScale([1_000, 1_100], 1)).toEqual({ min: 1_000, max: 1_100 })
  })

  it('rejects a spread below 1, which would invert the scale', () => {
    expect(() => latencyColorScale([1_000, 1_100], 0.5)).toThrow(/spread cannot be less than 1/)
  })

  it('defaults to a 500% spread', () => {
    expect(defaultLatencyColorSpread).toBe(5)
    expect(latencyColorScale([500])).toEqual({ min: 500, max: 2_500 })
  })

  it('collapses to a zero-width scale when every latency is zero', () => {
    expect(latencyColorScale([0, 0])).toEqual({ min: 0, max: 0 })
  })
})

describe('latencyColorFraction', () => {
  const scale = latencyColorScale([1_000, 9_000])

  it('is 0 at the bottom of the scale and 1 at the top', () => {
    expect(latencyColorFraction(1_000, scale)).toBe(0)
    expect(latencyColorFraction(9_000, scale)).toBe(1)
  })

  it('interpolates linearly in between', () => {
    expect(latencyColorFraction(5_000, scale)).toBeCloseTo(0.5)
  })

  it('clamps values outside the scale', () => {
    expect(latencyColorFraction(10, scale)).toBe(0)
    expect(latencyColorFraction(1_000_000, scale)).toBe(1)
  })

  it('leaves a lone connector at the bottom of the scale', () => {
    // A single connector cannot be an outlier relative to itself: the spread
    // floor puts the top of the scale at 3x its own latency.
    expect(latencyColorFraction(2_000, latencyColorScale([2_000]))).toBe(0)
  })

  it('keeps a tight cluster near the bottom rather than spanning to red', () => {
    const clustered = latencyColorScale([1_000, 1_100])
    // 1100 is 10% above the fastest, so it must stay well below saturation.
    expect(latencyColorFraction(1_100, clustered)).toBeCloseTo(0.025)
  })

  it('is 0 without a scale or a latency', () => {
    expect(latencyColorFraction(1_000, undefined)).toBe(0)
    expect(latencyColorFraction(null, scale)).toBe(0)
    expect(latencyColorFraction(undefined, scale)).toBe(0)
    expect(latencyColorFraction(NaN, scale)).toBe(0)
  })

  it('is 0 for a zero-width scale', () => {
    expect(latencyColorFraction(0, latencyColorScale([0, 0]))).toBe(0)
  })
})

describe('latencyColor', () => {
  const scale = latencyColorScale([1_000, 9_000])

  it('is fully the fast token at the bottom of the scale', () => {
    expect(latencyColor(1_000, scale)).toBe(
      'color-mix(in oklab, var(--latency-fast) 100.00%, var(--latency-slow) 0.00%)'
    )
  })

  it('is fully the slow token at the top of the scale', () => {
    expect(latencyColor(9_000, scale)).toBe(
      'color-mix(in oklab, var(--latency-fast) 0.00%, var(--latency-slow) 100.00%)'
    )
  })

  it('blends the two tokens in between', () => {
    expect(latencyColor(5_000, scale)).toBe(
      'color-mix(in oklab, var(--latency-fast) 50.00%, var(--latency-slow) 50.00%)'
    )
  })

  it('falls back to the fast token when there is no scale', () => {
    expect(latencyColor(1_000, undefined)).toBe(
      'color-mix(in oklab, var(--latency-fast) 100.00%, var(--latency-slow) 0.00%)'
    )
  })
})
