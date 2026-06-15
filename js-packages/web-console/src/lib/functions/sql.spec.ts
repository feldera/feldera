import { BigNumber } from 'bignumber.js'
import { describe, expect, it } from 'vitest'
import {
  bytesToHex,
  displaySQLValue,
  formatNonFiniteNumber,
  serializeSQLValue
} from '$lib/functions/sql'

describe('bytesToHex', () => {
  it('renders bytes as zero-padded lowercase hex', () => {
    expect(bytesToHex(new Uint8Array([0xde, 0xad, 0xbe, 0xef]))).toBe('deadbeef')
    // Low bytes must keep their leading zero (0x0a, not 0xa).
    expect(bytesToHex(new Uint8Array([0x00, 0x0a, 0x0f, 0xff]))).toBe('000a0fff')
    expect(bytesToHex(new Uint8Array([]))).toBe('')
  })
})

describe('displaySQLValue', () => {
  it('serializes binary (Uint8Array) as a hex string', () => {
    expect(displaySQLValue(new Uint8Array([0xde, 0xad, 0xbe, 0xef]))).toBe('deadbeef')
  })

  it('keeps the existing rendering for other value shapes', () => {
    expect(displaySQLValue(null)).toBe('NULL')
    expect(displaySQLValue('hello')).toBe('hello')
    expect(displaySQLValue(new BigNumber('123456789012345678.9012345678'))).toBe(
      '123456789012345678.9012345678'
    )
  })

  // Regression: JSON has no encoding for these IEEE-754 values, so the
  // `JSONbig.stringify` fallback used to collapse every one of them to the
  // literal `null` — indistinguishable from a SQL NULL. DOUBLE/REAL results
  // such as `1.0 / 0.0` must render as their actual value instead.
  it('renders non-finite floats rather than collapsing them to null', () => {
    expect(displaySQLValue(Infinity)).toBe('Infinity')
    expect(displaySQLValue(-Infinity)).toBe('-Infinity')
    expect(displaySQLValue(NaN)).toBe('NaN')
  })
})

describe('formatNonFiniteNumber', () => {
  it('spells out NaN and the infinities', () => {
    expect(formatNonFiniteNumber(NaN)).toBe('NaN')
    expect(formatNonFiniteNumber(Infinity)).toBe('Infinity')
    expect(formatNonFiniteNumber(-Infinity)).toBe('-Infinity')
  })

  it('leaves finite numbers and non-numbers for the normal path', () => {
    expect(formatNonFiniteNumber(0)).toBeUndefined()
    expect(formatNonFiniteNumber(-3.5)).toBeUndefined()
    expect(formatNonFiniteNumber(null)).toBeUndefined()
    expect(formatNonFiniteNumber('NaN')).toBeUndefined()
    expect(formatNonFiniteNumber(new BigNumber('1'))).toBeUndefined()
  })
})

describe('serializeSQLValue', () => {
  it('exports non-finite floats as their textual value, not null', () => {
    expect(serializeSQLValue(Infinity)).toBe('Infinity')
    expect(serializeSQLValue(-Infinity)).toBe('-Infinity')
    expect(serializeSQLValue(NaN)).toBe('NaN')
  })

  it('serializes ordinary values as JSON', () => {
    expect(serializeSQLValue(null)).toBe('null')
    expect(serializeSQLValue('hello')).toBe('"hello"')
    expect(serializeSQLValue(42)).toBe('42')
  })
})
