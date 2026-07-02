import { describe, expect, it, vi } from 'vitest'
import { promisePool } from './promise'

/** A promise whose resolution the test controls explicitly. */
const deferred = <T>() => {
  let resolve!: (value: T) => void
  let reject!: (reason?: unknown) => void
  const promise = new Promise<T>((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}

/**
 * Drain the microtask queue so resumed workers run to their next `await`.
 * Resolving a promise and resuming its awaiter spans a few microtask turns, so
 * yield several times rather than once.
 */
const flush = async () => {
  for (let i = 0; i < 5; i++) {
    await Promise.resolve()
  }
}

describe('promisePool', () => {
  it('returns results in input order regardless of completion order', async () => {
    const results = await promisePool([1, 2, 3, 4], 2, async (n) => n * 10)
    expect(results).toEqual([10, 20, 30, 40])
  })

  it('passes the item and its index to the worker', async () => {
    const seen: [string, number][] = []
    await promisePool(['a', 'b', 'c'], 2, async (item, index) => {
      seen.push([item, index])
    })
    expect(seen.sort((x, y) => x[1] - y[1])).toEqual([
      ['a', 0],
      ['b', 1],
      ['c', 2]
    ])
  })

  it('runs at most `concurrency` workers at once', async () => {
    let inFlight = 0
    let peak = 0
    const gates = Array.from({ length: 6 }, () => deferred<void>())
    const pool = promisePool(gates, 2, async (gate) => {
      inFlight++
      peak = Math.max(peak, inFlight)
      await gate.promise
      inFlight--
    })
    // Release the gates one at a time, letting the pool refill its slots.
    for (const gate of gates) {
      await flush()
      gate.resolve()
    }
    await pool
    expect(peak).toBe(2)
  })

  it('slides the window: a slow item never stalls an idle slot', async () => {
    const started: number[] = []
    const gates = [deferred<void>(), deferred<void>(), deferred<void>(), deferred<void>()]
    const pool = promisePool([0, 1, 2, 3], 2, async (n) => {
      started.push(n)
      await gates[n].promise
    })

    // Slots 0 and 1 start immediately; 2 and 3 wait for a free slot.
    await flush()
    expect(started).toEqual([0, 1])

    // Finish item 1 (the second slot); item 2 should take its place at once,
    // even though item 0 is still running.
    gates[1].resolve()
    await flush()
    expect(started).toEqual([0, 1, 2])

    gates[0].resolve()
    await flush()
    expect(started).toEqual([0, 1, 2, 3])

    gates[2].resolve()
    gates[3].resolve()
    await pool
  })

  it('handles an empty input without invoking the worker', async () => {
    const fn = vi.fn(async (n: number) => n)
    const results = await promisePool([], 4, fn)
    expect(results).toEqual([])
    expect(fn).not.toHaveBeenCalled()
  })

  it('caps workers at the item count when concurrency exceeds it', async () => {
    let inFlight = 0
    let peak = 0
    await promisePool([1, 2], 10, async (n) => {
      inFlight++
      peak = Math.max(peak, inFlight)
      await flush()
      inFlight--
      return n
    })
    expect(peak).toBe(2)
  })

  it('clamps non-positive concurrency to a single worker', async () => {
    let inFlight = 0
    let peak = 0
    const results = await promisePool([1, 2, 3], 0, async (n) => {
      inFlight++
      peak = Math.max(peak, inFlight)
      await flush()
      inFlight--
      return n
    })
    expect(peak).toBe(1)
    expect(results).toEqual([1, 2, 3])
  })

  it('rejects when a worker rejects, and starts no further items', async () => {
    const started: number[] = []
    await expect(
      promisePool([0, 1, 2, 3, 4, 5], 2, async (n) => {
        started.push(n)
        if (n === 1) {
          throw new Error('boom')
        }
        // Item 0 never settles on its own, so the only progress past the first
        // two is whatever the failing worker would have pulled next — and it
        // must pull nothing once the pool is rejecting.
        await new Promise<void>(() => {})
      })
    ).rejects.toThrow('boom')
    // Only the initial two items ever started; no third item was claimed.
    expect(started).toEqual([0, 1])
  })
})
