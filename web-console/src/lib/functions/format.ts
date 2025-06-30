import Dayjs from 'dayjs'
import { useInterval } from '$lib/compositions/common/useInterval.svelte'

export const useElapsedTime = () => {
  const now = useInterval(() => new Date(), 1000, 1000 - (Date.now() % 1000))
  const formatElapsedTime = (timestamp: Date, precision: 'dhms' | 'dhm' = 'dhms') => {
    const delta = now.current.valueOf() - timestamp.valueOf()
    const d = Dayjs.duration(delta)
    return (
      ((d) => (d ? ` ${d}d` : ''))(Math.floor(d.asDays())) +
      ((d) => (d ? ` ${d}h` : ''))(d.hours()) +
      ((d) => (d ? ` ${d}m` : ''))(d.minutes()) +
      (precision.includes('s')
        ? ((d) => (d ? ` ${d}s` : ''))(d.seconds())
        : delta > 60000
          ? ''
          : '< 1m')
    )
  }
  return { formatElapsedTime }
}
