import { describe, expect, it } from 'vitest'
import { formatQty } from './formatQty.js'

describe('formatQty', () => {
  it('formats integers with comma grouping by default', () => {
    expect(formatQty(999)).toBe('999')
    expect(formatQty(12345)).toBe('12,345')
    expect(formatQty(1234567)).toBe('1,234,567')
    expect(formatQty(0)).toBe('0')
  })

  it('rounds fractional inputs to whole numbers', () => {
    expect(formatQty(12.7)).toBe('13')
  })

  it("uses 3 significant digits with an SI suffix in 'rounded' mode once >= 1000", () => {
    expect(formatQty(12345, 'rounded')).toBe('12.3k')
    expect(formatQty(1500, 'rounded')).toBe('1.50k')
    // Below 1000 the rounded flag keeps the plain grouped integer.
    expect(formatQty(999, 'rounded')).toBe('999')
  })

  it('renders an em dash for non-finite / missing values', () => {
    expect(formatQty(null)).toBe('—')
    expect(formatQty(undefined)).toBe('—')
    expect(formatQty(Number.NaN)).toBe('—')
    expect(formatQty(Number.POSITIVE_INFINITY)).toBe('—')
  })
})
