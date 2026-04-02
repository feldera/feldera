import { describe, expect, it } from 'vitest'
import { matchesSubstring } from './string'

describe('matchesSubstring', () => {
  it('returns true when search is empty', () => {
    expect(matchesSubstring('any-pipeline', '')).toBe(true)
  })

  it('matches exact name', () => {
    expect(matchesSubstring('my-pipeline', 'my-pipeline')).toBe(true)
  })

  it('matches partial name at the start', () => {
    expect(matchesSubstring('my-pipeline', 'my-')).toBe(true)
  })

  it('matches partial name in the middle', () => {
    expect(matchesSubstring('my-pipeline', 'pipe')).toBe(true)
  })

  it('matches partial name at the end', () => {
    expect(matchesSubstring('my-pipeline', 'line')).toBe(true)
  })

  it('is case-insensitive (lowercase search, mixed case text)', () => {
    expect(matchesSubstring('My-Pipeline', 'my-pipeline')).toBe(true)
  })

  it('is case-insensitive (uppercase search)', () => {
    expect(matchesSubstring('my-pipeline', 'MY-PIPE')).toBe(true)
  })

  it('returns false when there is no match', () => {
    expect(matchesSubstring('my-pipeline', 'xyz')).toBe(false)
  })

  it('returns false for partial mismatch', () => {
    expect(matchesSubstring('my-pipeline', 'my-pipez')).toBe(false)
  })
})
