import { describe, expect, it } from 'vitest'
import { formatQty } from './format'

describe('formatQty', () => {
  it('returns em dash for null', () => {
    expect(formatQty(null)).toBe('—')
  })

  it('returns em dash for undefined', () => {
    expect(formatQty(undefined)).toBe('—')
  })

  it('returns em dash for NaN', () => {
    expect(formatQty(NaN)).toBe('—')
  })

  it('returns em dash for Infinity', () => {
    expect(formatQty(Infinity)).toBe('—')
  })

  it('returns em dash for -Infinity', () => {
    expect(formatQty(-Infinity)).toBe('—')
  })

  it('formats zero', () => {
    expect(formatQty(0)).toBe('0')
  })

  it('formats a small number without commas', () => {
    expect(formatQty(999)).toBe('999')
  })

  it('formats a number >= 1000 with comma separator when not rounded', () => {
    expect(formatQty(1000)).toBe('1,000')
  })

  it('formats a large number with comma separators when not rounded', () => {
    expect(formatQty(1234567)).toBe('1,234,567')
  })

  it('formats a number >= 1000 with SI suffix when rounded', () => {
    expect(formatQty(1000, 'rounded')).toBe('1.00k')
  })

  it('formats a number < 1000 without SI suffix even when rounded', () => {
    expect(formatQty(999, 'rounded')).toBe('999')
  })

  it('formats a large number with SI suffix when rounded', () => {
    expect(formatQty(1500000, 'rounded')).toBe('1.50M')
  })
})
