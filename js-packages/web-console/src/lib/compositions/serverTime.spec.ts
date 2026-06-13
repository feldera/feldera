import { describe, expect, it } from 'vitest'
import { ServerDate } from './serverTime'

describe('ServerDate', () => {
  it('behaves like Date when constructed with an explicit value', () => {
    expect(new ServerDate(0).getTime()).toBe(0)
    expect(new ServerDate('2020-01-01T00:00:00Z').getTime()).toBe(
      Date.parse('2020-01-01T00:00:00Z')
    )
    expect(new ServerDate() instanceof Date).toBe(true)
  })

  it('applies the synced server/client offset to now()', () => {
    // Pretend the server clock is one minute ahead of this client.
    ServerDate.sync(Date.now() + 60_000)
    const skew = ServerDate.now() - Date.now()
    expect(skew).toBeGreaterThanOrEqual(59_000)
    expect(skew).toBeLessThanOrEqual(61_000)
  })

  it('reflects the synced offset in a zero-argument instance', () => {
    ServerDate.sync(Date.now() + 60_000)
    const skew = new ServerDate().getTime() - Date.now()
    expect(skew).toBeGreaterThanOrEqual(59_000)
    expect(skew).toBeLessThanOrEqual(61_000)
  })
})
