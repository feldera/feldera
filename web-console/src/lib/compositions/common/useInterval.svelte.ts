/**
 * Runs the function on initial call
 * @param f
 * @param durationMs
 * @param offsetMs
 * @returns
 */
export const useInterval = <T>(f: () => T, durationMs: number, offsetMs?: number) => {
  let state = $state(f())
  let interval: Timer
  $effect.pre(() => {
    if (offsetMs) {
      setTimeout(() => (interval = setInterval(() => (state = f()), durationMs)), offsetMs)
    } else {
      interval = setInterval(() => (state = f()), durationMs)
    }
    return () => {
      clearInterval(interval)
    }
  })
  return {
    get current() {
      return state
    }
  }
}
