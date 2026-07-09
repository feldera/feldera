import { describe, expect, it } from 'vitest'
import { niceTicks } from './histogramTicks'

describe('niceTicks', () => {
  it('returns empty for a non-positive max', () => {
    expect(niceTicks(0)).toEqual([])
    expect(niceTicks(-5)).toEqual([])
    expect(niceTicks(Number.NaN)).toEqual([])
  })

  it('produces round multiples of 5 / 10 at a larger scale', () => {
    expect(niceTicks(1500)).toEqual([0, 500, 1000, 1500])
    expect(niceTicks(1800)).toEqual([0, 500, 1000, 1500]) // top nice tick may be below max
  })

  it('never exceeds maxCount, thinning the step when needed', () => {
    for (const max of [5, 8, 23, 47, 99, 250, 1234, 9999, 55000]) {
      expect(niceTicks(max).length).toBeLessThanOrEqual(5)
    }
  })

  it('always starts at 0 and stays within [0, max]', () => {
    const t = niceTicks(1234)
    expect(t[0]).toBe(0)
    expect(Math.max(...t)).toBeLessThanOrEqual(1234)
  })

  it('keeps ticks integral when integer=true (no fractional counts)', () => {
    const t = niceTicks(2, 5, true)
    expect(t).toEqual([0, 1, 2])
    for (const v of niceTicks(8, 5, true)) {
      expect(Number.isInteger(v)).toBe(true)
    }
  })

  it('allows fractional steps when integer=false', () => {
    expect(niceTicks(2, 5, false)).toContain(0.5)
  })

  it('honors a custom maxCount', () => {
    expect(niceTicks(100, 3).length).toBeLessThanOrEqual(3)
  })
})
