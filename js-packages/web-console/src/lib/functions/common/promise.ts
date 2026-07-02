import { clearTimeout as clearWorkerTimeout, setTimeout as setWorkerTimeout } from 'worker-timers'

export const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

/**
 * Map over `items` with a bounded number of concurrent workers — a sliding
 * window rather than fixed batches. At most `concurrency` calls to `fn` are in
 * flight at once; as each settles, the next item starts immediately.
 *
 * Results are returned in input order, like `Promise.all`. `fn` receives the
 * item and its index. A rejection from `fn` rejects the whole pool (again like
 * `Promise.all`), and no further items are started; callers that want to finish
 * the rest should catch inside `fn`.
 *
 * The "workers" are not threads: everything runs on the single main JS thread,
 * so this is concurrency, not parallelism. It only helps when `fn` spends its
 * time awaiting (a network request, a timer) - those waits overlap. CPU-bound
 * `fn`s gain nothing, since the thread still runs them one at a time.
 *
 * @param items       Items to process.
 * @param concurrency Maximum number of `fn` calls in flight at once (clamped to ≥ 1).
 * @param fn          Async worker invoked once per item.
 * @returns Results in the same order as `items`.
 */
export const promisePool = async <T, R>(
  items: readonly T[],
  concurrency: number,
  fn: (item: T, index: number) => Promise<R>
): Promise<R[]> => {
  const results = new Array<R>(items.length)
  let next = 0
  // Each worker pulls the next unclaimed index until the list is drained;
  // `next++` is atomic between awaits, so no two workers claim the same item.
  const worker = async () => {
    while (next < items.length) {
      const index = next++
      results[index] = await fn(items[index], index)
    }
  }
  const workerCount = Math.max(1, Math.min(concurrency, items.length))
  await Promise.all(Array.from({ length: workerCount }, worker))
  return results
}

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
    if (state.name !== 'scheduled') {
      return
    }
    state = { name: 'running' }
    Promise.resolve()
      .then(action)
      .catch(() => {
        /* swallow — rejection handling is the caller's responsibility */
      })
      .finally(afterTick)
  }

  const afterTick = () => {
    if (state.name !== 'running') {
      return
    }
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
