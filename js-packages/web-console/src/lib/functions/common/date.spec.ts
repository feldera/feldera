import { describe, expect, it } from 'vitest'
import { uuidV7Timestamp } from './date'

describe('uuidV7Timestamp', () => {
  it('returns null for a non-v7 UUID', () => {
    expect(uuidV7Timestamp('550e8400-e29b-41d4-a716-446655440000')).toBeNull()
  })

  it('extracts the correct timestamp from a v7 UUID', () => {
    // 01900000-0000 encodes unix ms 0x019000000000 = 1717986918400
    expect(uuidV7Timestamp('01900000-0000-7000-8000-000000000000')?.valueOf()).toBe(0x019000000000)
  })
})
