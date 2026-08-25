import Dayjs from 'dayjs'
import { useInterval } from '$lib/compositions/common/useInterval.svelte'
import { ServerDate } from '$lib/compositions/serverTime'

export const useElapsedTime = () => {
  // Every timestamp measured here comes from the server, so ageing it against a skewed
  // browser clock would report drift as elapsed time.
  const now = useInterval(() => new ServerDate(), 1000, 1000 - (Date.now() % 1000))
  const formatElapsedTime = (timestamp: Date, precision: 'dhms' | 'dhm' = 'dhms') => {
    const delta = now.current.valueOf() - timestamp.valueOf()
    const d = Dayjs.duration(delta)
    return (
      ((d) => (d ? ` ${d}d` : ''))(Math.max(Math.floor(d.asDays()), 0)) +
      ((d) => (d ? ` ${d}h` : ''))(d.hours()) +
      ((d) => (d ? ` ${d}m` : ''))(d.minutes()) +
      (precision.includes('s')
        ? ((d) => (d ? ` ${d}s` : ''))(d.seconds())
        : delta > 60000
          ? ''
          : '< 1m')
    )
  }
  const formatUpdatedAgo = (timestamp: Date | null | undefined, stepMs = 10_000): string | null => {
    if (!timestamp) {
      return null
    }
    const elapsed = now.current.valueOf() - timestamp.valueOf()
    const steps = Math.floor(elapsed / stepMs)
    const seconds = steps * (stepMs / 1000)
    return seconds === 0 ? 'updated just now' : `updated ${seconds} seconds ago`
  }
  return { formatElapsedTime, formatUpdatedAgo }
}
