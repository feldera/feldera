import { closedIntervalAction } from '$lib/functions/common/promise'

/**
 * Runs `f` immediately, then repeatedly on an interval, exposing the latest return value
 * as reactive `.current`.
 *
 * Built on {@link closedIntervalAction}, so polling is paused while the document is
 * hidden and async callbacks are awaited before the next tick is scheduled.
 */
export const useInterval = <T>(f: () => T, durationMs: number, offsetMs?: number) => {
  let state = $state(f())

  const action = async () => {
    const result = f()
    if (result && typeof (result as { then?: unknown }).then === 'function') {
      state = await (result as unknown as Promise<T>)
    } else {
      state = result
    }
  }

  $effect.pre(() => closedIntervalAction(action, durationMs, { initialDelayMs: offsetMs }))

  return {
    get current() {
      return state
    }
  }
}
