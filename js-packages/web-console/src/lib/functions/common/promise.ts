export const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

/**
 * Start calling an action in a loop with an interval while waiting for the previous invocation to resolve
 * Does not perform the action on the initial call
 * Does not handle rejection
 * @returns A function to cancel the action loop
 */
export const closedIntervalAction = (action: () => Promise<void>, periodMs: number) => {
  let cancelled = false
  let t0 = Date.now()
  let timeout: Timer

  function execute() {
    if (cancelled) {
      return
    }

    action().finally(() => {
      if (cancelled) {
        return
      }

      const now = Date.now()
      t0 += periodMs
      const nextDelay = Math.max(t0 - now, 0)

      timeout = setTimeout(execute, nextDelay)
    })
  }

  timeout = setTimeout(execute, periodMs)

  return () => {
    cancelled = true
    clearTimeout(timeout)
  }
}
