import { describe, expect, it } from 'vitest'
import { type Microseconds, microseconds } from './duration'

describe('microseconds', () => {
  it('carries the value through unchanged', () => {
    // The brand is erased at runtime: only the type distinguishes the unit.
    expect(microseconds(1_200)).toBe(1_200)
  })

  it('keeps a missing value missing', () => {
    expect(microseconds(null)).toBeNull()
    expect(microseconds(undefined)).toBeUndefined()
  })

  it('brands a number and passes null through at the type level', () => {
    // Both lines typecheck only while `SameNullability` maps a number to
    // `Microseconds` and leaves `null` alone.
    const branded: Microseconds = microseconds(1_200)
    const missing: null = microseconds(null)
    expect([branded, missing]).toEqual([1_200, null])
  })
})
