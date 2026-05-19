import { BigNumber } from 'bignumber.js'
import { describe, expect, it } from 'vitest'
import { bytesToHex, displaySQLValue } from '$lib/functions/sql'

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
})
