import { describe, expect, it } from 'vitest'
import { batchRange, fractionIn } from './statsMetric.js'

describe('fractionIn', () => {
  it('maps a value to its clamped fraction within the range', () => {
    expect(fractionIn(10, 10, 100)).toBe(0)
    expect(fractionIn(100, 10, 100)).toBe(1)
    expect(fractionIn(55, 10, 100)).toBeCloseTo(0.5, 10)
  })

  it('clamps out-of-range values to [0, 1]', () => {
    expect(fractionIn(-5, 0, 10)).toBe(0)
    expect(fractionIn(20, 0, 10)).toBe(1)
  })

  it('returns 0 for a degenerate range', () => {
    expect(fractionIn(42, 42, 42)).toBe(0)
    expect(fractionIn(5, 10, 10)).toBe(0)
  })
})

describe('batchRange', () => {
  it('takes the smallest min and largest max across workers', () => {
    const r = batchRange(
      [
        { min: 30, avg: 50, max: 90 },
        { min: 10, avg: 40, max: 200 }
      ],
      10,
      450
    )
    expect(r.min).toBe(10)
    expect(r.max).toBe(200)
    expect(r.avg).toBe(45) // 450 records / 10 batches
  })

  it('skips missing per-worker fields', () => {
    const r = batchRange([{ avg: 5 }, { min: 3, max: 7 }, {}], 4, 20)
    expect(r.min).toBe(3)
    expect(r.max).toBe(7)
    expect(r.avg).toBe(5)
  })

  it('is well-defined with no batches or no workers', () => {
    expect(batchRange([], 0, 0)).toEqual({ min: 0, max: 0, avg: 0 })
    // No records but nonzero batches -> average 0, not NaN.
    expect(batchRange([{ min: 1, max: 2 }], 3, 0)).toEqual({ min: 1, max: 2, avg: 0 })
  })
})
