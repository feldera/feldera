import { clearTimeout as clearWorkerTimeout, setTimeout as setWorkerTimeout } from 'worker-timers'

export const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

const hasDocument = typeof document !== 'undefined'
const isHidden = () => hasDocument && document.hidden
const clearWorkerTimeoutSafely = (timeout: number) => {
  try {
    clearWorkerTimeout(timeout)
  } catch {
    // timer may have already fired
  }
}

export type ClosedIntervalOptions = {
  /** Delay before the first invocation. Defaults to `periodMs`. */
  initialDelayMs?: number
}

/**
 * Start calling an action in a loop with an interval while waiting for the previous
 * invocation to resolve. A sync action's return value is ignored; a rejected promise is
 * swallowed (same semantics the callers relied on before).
 *
 * Polling is paused while the document is hidden (Page Visibility API) to prevent
 * browser throttling from queueing up missed ticks that would then fire as a burst when
 * the tab regains focus. When the tab becomes visible again the action runs immediately
 * and the cadence resumes relative to that moment.
 *
 * Uses `worker-timers` so the scheduler ticks in a dedicated Web Worker and is not
 * subject to main-thread throttling while the tab is visible but resource-starved.
 *
 * @returns A function to cancel the action loop.
 */
export const closedIntervalAction = (
  action: () => void | Promise<void>,
  periodMs: number,
  options: ClosedIntervalOptions = {}
) => {
  // Lifecycle state machine. Transitions:
  //   idle      → scheduled (schedule while visible)
  //   idle      → paused    (schedule while hidden)
  //   scheduled → running   (timer fires)
  //   running   → idle      (action resolved — then re-schedules)
  //   scheduled → paused    (visibility → hidden)
  //   paused    → scheduled (visibility → visible)
  //   any       → cancelled (caller disposed)
  type State =
    | { name: 'idle' }
    | { name: 'scheduled'; timeout: number }
    | { name: 'running' }
    | { name: 'paused' }
    | { name: 'cancelled' }

  let state: State = { name: 'idle' }
  let nextAt = Date.now() + periodMs

  const schedule = (delay: number) => {
    if (state.name !== 'idle' && state.name !== 'paused') {
      return
    }
    if (isHidden()) {
      state = { name: 'paused' }
      return
    }
    state = { name: 'scheduled', timeout: setWorkerTimeout(tick, delay) }
  }

  const tick = () => {
    if (state.name !== 'scheduled') return
    state = { name: 'running' }
    Promise.resolve()
      .then(action)
      .catch(() => {
        /* swallow — rejection handling is the caller's responsibility */
      })
      .finally(afterTick)
  }

  const afterTick = () => {
    if (state.name !== 'running') return
    const now = Date.now()
    nextAt += periodMs
    // If we fell far behind (e.g. action took longer than one period), reset cadence
    // to `now` instead of firing a burst of catch-up ticks.
    if (now - nextAt > periodMs) {
      nextAt = now + periodMs
    }
    state = { name: 'idle' }
    schedule(Math.max(nextAt - now, 0))
  }

  const onVisibilityChange = () => {
    if (isHidden()) {
      if (state.name === 'scheduled') {
        clearWorkerTimeoutSafely(state.timeout)
        state = { name: 'paused' }
      }
      // In `running`, afterTick will observe isHidden() via schedule() and park in paused.
      return
    }
    if (state.name === 'paused') {
      // Anchor cadence to `now` so that after the immediate resume tick advances
      // `nextAt` by one period in afterTick, the next tick fires one period later
      // (not two).
      nextAt = Date.now()
      state = { name: 'idle' }
      schedule(0)
    }
  }

  schedule(options.initialDelayMs ?? periodMs)
  if (hasDocument) {
    document.addEventListener('visibilitychange', onVisibilityChange)
  }

  return () => {
    if (state.name === 'scheduled') {
      clearWorkerTimeoutSafely(state.timeout)
    }
    state = { name: 'cancelled' }
    if (hasDocument) {
      document.removeEventListener('visibilitychange', onVisibilityChange)
    }
  }
}
