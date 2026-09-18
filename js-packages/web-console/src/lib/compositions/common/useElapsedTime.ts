import Dayjs from 'dayjs'
import { useInterval } from '$lib/compositions/common/useInterval.svelte'
import { ServerDate } from '$lib/compositions/serverTime'

/**
 * A duration as ` 2d 3h 4m 5s`, leaving out the components that are zero; the empty string
 * for less than a second. Precision `dhm` stops at minutes and renders anything shorter
 * than one as `< 1m`.
 */
export const formatDuration = (deltaMs: number, precision: 'dhms' | 'dhm' = 'dhms') => {
  const d = Dayjs.duration(deltaMs)
  return (
    ((d) => (d ? ` ${d}d` : ''))(Math.max(Math.floor(d.asDays()), 0)) +
    ((d) => (d ? ` ${d}h` : ''))(d.hours()) +
    ((d) => (d ? ` ${d}m` : ''))(d.minutes()) +
    (precision.includes('s')
      ? ((d) => (d ? ` ${d}s` : ''))(d.seconds())
      : deltaMs >= 60000
        ? ''
        : '< 1m')
  )
}

export const useElapsedTime = () => {
  // Every timestamp measured here comes from the server, so ageing it against a skewed
  // browser clock would report drift as elapsed time.
  const now = useInterval(() => new ServerDate(), 1000, 1000 - (Date.now() % 1000))
  const formatElapsedTime = (timestamp: Date, precision: 'dhms' | 'dhm' = 'dhms') =>
    formatDuration(now.current.valueOf() - timestamp.valueOf(), precision)
  /**
   * How long ago a view last refreshed, quantized to `stepMs` so that the label does not
   * tick every second. A cluster monitor event is up to ten minutes old, and older still
   * once the monitor dies, so the age rolls over into minutes and hours.
   */
  const formatUpdatedAgo = (timestamp: Date | null | undefined, stepMs = 10_000): string | null => {
    if (!timestamp) {
      return null
    }
    // A timestamp ahead of the clock ages to nothing rather than to a negative duration.
    const elapsed = Math.max(now.current.valueOf() - timestamp.valueOf(), 0)
    const ago = formatDuration(Math.floor(elapsed / stepMs) * stepMs).trim()
    return ago ? `updated ${ago} ago` : 'updated just now'
  }
  return { formatElapsedTime, formatUpdatedAgo }
}
