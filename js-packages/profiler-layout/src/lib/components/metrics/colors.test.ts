import { describe, expect, it } from 'vitest'
import { barMetrics, logScale01 } from './colors.js'

describe('barMetrics', () => {
  it('maps the range endpoints to the height envelope', () => {
    // v === min -> t 0 -> minH; v === max -> t 1 -> maxH.
    expect(barMetrics(10, 10, 100, 6, 24)).toEqual({ t: 0, height: 6 })
    expect(barMetrics(100, 10, 100, 6, 24)).toEqual({ t: 1, height: 24 })
  })

  it('is log-scaled between the endpoints (concave, emphasizes small values)', () => {
    const mid = barMetrics(55, 10, 100, 6, 24) // raw 0.5
    expect(mid.t).toBeCloseTo(logScale01(0.5), 10)
    // Concave: the midpoint value sits above the linear halfway height.
    expect(mid.height).toBeGreaterThan(6 + (24 - 6) * 0.5)
  })

  it('treats a degenerate range (no spread across workers) as a flat minimal bar', () => {
    // A relative-difference chart has nothing to show when every worker is equal: t 0, not maxH.
    expect(barMetrics(42, 42, 42)).toEqual({ t: 0, height: 6 })
  })

  it('honors custom height bounds', () => {
    expect(barMetrics(100, 0, 100, 12, 32)).toEqual({ t: 1, height: 32 })
    expect(barMetrics(0, 0, 100, 12, 32)).toEqual({ t: 0, height: 12 })
  })
})
