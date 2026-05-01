import Dayjs from 'dayjs'
import { useInterval } from '$lib/compositions/common/useInterval.svelte'

export const useElapsedTime = () => {
  const now = useInterval(() => new Date(), 1000, 1000 - (Date.now() % 1000))
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
  return { formatElapsedTime }
}

export const formatDateTime = (
  timestamp: Date | Dayjs.Dayjs | { ms: number },
  format = 'MMM D, YYYY h:mm A'
) => {
  return Dayjs('ms' in timestamp ? timestamp.ms : timestamp).format(format)
}

/**
 * Format a `[from, to]` timestamp range, omitting redundant parts on the
 * right-hand side: when both endpoints fall on the same calendar day only the
 * time is rendered, otherwise when they share a year the year is dropped.
 */
export const formatDateTimeRange = (
  from: Date | Dayjs.Dayjs | { ms: number },
  to: Date | Dayjs.Dayjs | { ms: number }
) => {
  const a = Dayjs('ms' in from ? from.ms : from)
  const b = Dayjs('ms' in to ? to.ms : to)
  const left = a.format('MMM D, YYYY h:mm A')
  const rightFmt = a.isSame(b, 'day')
    ? 'h:mm A'
    : a.isSame(b, 'year')
      ? 'MMM D, h:mm A'
      : 'MMM D, YYYY h:mm A'
  return `${left} - ${b.format(rightFmt)}`
}
