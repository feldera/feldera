export const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

/**
 * Start calling an action in a loop with an interval while waiting for the previous invocation to resolve
 * Does not perform the action on the initial call
 * Does not handle rejection
 * @returns A function to cancel the action loop
 */
export const closedIntervalAction = (action: () => Promise<void>, periodMs: number) => {
  let onTimeoutReject: (reason?: any) => void
  let t1 = Date.now()
  setTimeout(async () => {
    try {
      while (true) {
        await new Promise((resolve, reject) => {
          const t2 = Date.now()
          setTimeout(resolve, Math.max(Math.min(periodMs - t2 + t1, periodMs), 0))
          onTimeoutReject = reject
        })
        t1 = Date.now()
        await action()
      }
    } catch (e) {
      if (e === 'Closed interval cancelled') {
        return
      }
      throw e
    }
  })
  return () => {
    onTimeoutReject('Closed interval cancelled')
  }
}
