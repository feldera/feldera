// Named `.svelte.spec.ts` so it runs under the `client` project (real browser via
// Playwright) — the test needs `document`, `visibilitychange` events, and worker-timers'
// Web Worker. No Svelte component is rendered here.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

// Replace worker-timers with thin wrappers over the global timer API so that
// `vi.useFakeTimers()` can deterministically drive time in the test.
vi.mock('worker-timers', () => ({
  setTimeout: (fn: () => void, ms: number) => globalThis.setTimeout(fn, ms) as unknown as number,
  clearTimeout: (id: number) =>
    globalThis.clearTimeout(id as unknown as ReturnType<typeof setTimeout>)
}))

import { closedIntervalAction } from './promise'

const setHidden = (hidden: boolean) => {
  Object.defineProperty(document, 'hidden', { configurable: true, get: () => hidden })
  Object.defineProperty(document, 'visibilityState', {
    configurable: true,
    get: () => (hidden ? 'hidden' : 'visible')
  })
  document.dispatchEvent(new Event('visibilitychange'))
}

describe('closedIntervalAction', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    setHidden(false)
  })

  afterEach(() => {
    vi.useRealTimers()
    setHidden(false)
  })

  it('calls the action every period while the tab is visible', async () => {
    const action = vi.fn(async () => {})
    const cancel = closedIntervalAction(action, 1000)

    expect(action).not.toHaveBeenCalled()
    await vi.advanceTimersByTimeAsync(1000)
    expect(action).toHaveBeenCalledTimes(1)
    await vi.advanceTimersByTimeAsync(1000)
    expect(action).toHaveBeenCalledTimes(2)

    cancel()
  })

  it('respects `initialDelayMs` for the first tick', async () => {
    const action = vi.fn(async () => {})
    const cancel = closedIntervalAction(action, 1000, { initialDelayMs: 50 })

    await vi.advanceTimersByTimeAsync(49)
    expect(action).not.toHaveBeenCalled()
    await vi.advanceTimersByTimeAsync(1)
    expect(action).toHaveBeenCalledTimes(1)

    cancel()
  })

  it('does not call the action while the tab is hidden', async () => {
    const action = vi.fn(async () => {})
    const cancel = closedIntervalAction(action, 1000)

    setHidden(true)
    await vi.advanceTimersByTimeAsync(60_000)
    expect(action).not.toHaveBeenCalled()

    cancel()
  })

  it('pauses an already-armed timer when the tab is hidden mid-period', async () => {
    const action = vi.fn(async () => {})
    const cancel = closedIntervalAction(action, 1000)

    await vi.advanceTimersByTimeAsync(500) // half-way through the first period
    setHidden(true)
    await vi.advanceTimersByTimeAsync(60_000)
    expect(action).not.toHaveBeenCalled()

    cancel()
  })

  it('fires a single tick on visibility return — never a burst of catch-up ticks', async () => {
    const action = vi.fn(async () => {})
    const cancel = closedIntervalAction(action, 1000)

    setHidden(true)
    await vi.advanceTimersByTimeAsync(60_000) // tab hidden for 60 periods
    expect(action).not.toHaveBeenCalled()

    setHidden(false)
    await vi.advanceTimersByTimeAsync(0) // flush the scheduled(0) tick
    expect(action).toHaveBeenCalledTimes(1) // exactly one, not 60

    // And the cadence resumes relative to now, not the original t0.
    await vi.advanceTimersByTimeAsync(999)
    expect(action).toHaveBeenCalledTimes(1)
    await vi.advanceTimersByTimeAsync(1)
    expect(action).toHaveBeenCalledTimes(2)

    cancel()
  })

  it('awaits the previous invocation before scheduling the next', async () => {
    let resolveFirst!: () => void
    const firstDone = new Promise<void>((r) => {
      resolveFirst = r
    })
    let calls = 0
    const action = vi.fn(async () => {
      calls++
      if (calls === 1) await firstDone
    })

    const cancel = closedIntervalAction(action, 1000)

    await vi.advanceTimersByTimeAsync(1000)
    expect(action).toHaveBeenCalledTimes(1)

    // Even after multiple periods pass, the next tick is not scheduled
    // until the first action resolves.
    await vi.advanceTimersByTimeAsync(5000)
    expect(action).toHaveBeenCalledTimes(1)

    resolveFirst()
    // Microtask + one full period for the next scheduled tick.
    await vi.advanceTimersByTimeAsync(1000)
    expect(action).toHaveBeenCalledTimes(2)

    cancel()
  })

  it('a rejection in the action does not break the loop', async () => {
    let calls = 0
    const action = vi.fn(async () => {
      calls++
      if (calls === 1) throw new Error('boom')
    })

    const cancel = closedIntervalAction(action, 1000)

    await vi.advanceTimersByTimeAsync(1000)
    expect(action).toHaveBeenCalledTimes(1)
    await vi.advanceTimersByTimeAsync(1000)
    expect(action).toHaveBeenCalledTimes(2)

    cancel()
  })

  it('cancel while hidden prevents a tick when the tab later becomes visible', async () => {
    const action = vi.fn(async () => {})
    const cancel = closedIntervalAction(action, 1000)

    setHidden(true)
    await vi.advanceTimersByTimeAsync(2000)
    cancel()

    setHidden(false)
    await vi.advanceTimersByTimeAsync(5000)
    expect(action).not.toHaveBeenCalled()
  })

  it('cancel mid-flight prevents the next tick from scheduling', async () => {
    let resolveAction!: () => void
    const blocking = new Promise<void>((r) => {
      resolveAction = r
    })
    const action = vi.fn(async () => {
      await blocking
    })

    const cancel = closedIntervalAction(action, 1000)

    await vi.advanceTimersByTimeAsync(1000)
    expect(action).toHaveBeenCalledTimes(1) // running

    cancel()
    resolveAction()

    await vi.advanceTimersByTimeAsync(5000)
    expect(action).toHaveBeenCalledTimes(1) // never re-scheduled
  })
})
