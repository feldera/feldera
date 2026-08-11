import { describe, expect, it } from 'vitest'
import { microseconds } from './common/duration'
import { formatDuration, formatQty } from './format'

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

describe('formatDuration', () => {
  it('renders zero without a unit', () => {
    expect(formatDuration(microseconds(0))).toBe('0')
  })

  it('uses microseconds below 1ms', () => {
    expect(formatDuration(microseconds(340))).toBe('340 µs')
    expect(formatDuration(microseconds(999))).toBe('999 µs')
  })

  it('switches to milliseconds at 1000µs', () => {
    expect(formatDuration(microseconds(1_000))).toBe('1 ms')
    expect(formatDuration(microseconds(1_200))).toBe('1.2 ms')
    expect(formatDuration(microseconds(12_500))).toBe('12.5 ms')
    expect(formatDuration(microseconds(340_000))).toBe('340 ms')
  })

  it('switches to seconds at 1_000_000µs', () => {
    expect(formatDuration(microseconds(1_000_000))).toBe('1 s')
    expect(formatDuration(microseconds(2_100_000))).toBe('2.1 s')
    expect(formatDuration(microseconds(90_000_000))).toBe('90 s')
  })

  it('returns an em dash for non-finite input', () => {
    expect(formatDuration(undefined)).toBe('—')
    expect(formatDuration(null)).toBe('—')
    expect(formatDuration(microseconds(NaN))).toBe('—')
  })

  it('passes a missing latency through to the em dash', () => {
    // `microseconds` keeps null and undefined, so callers brand a nullable
    // field without unwrapping it first.
    expect(formatDuration(microseconds(null))).toBe('—')
    expect(formatDuration(microseconds(undefined))).toBe('—')
  })

  it('rejects an unbranded number', () => {
    // @ts-expect-error - durations must be branded with `microseconds`
    const formatted = formatDuration(1_200)
    // The rejection is compile-time only, so the call still formats. The guard
    // is the directive above: svelte-check fails on an unused @ts-expect-error,
    // which is what a widened signature would produce.
    expect(formatted).toBe('1.2 ms')
  })
})
