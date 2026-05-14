import { format } from 'd3-format'
import Dayjs from 'dayjs'

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

export const formatQty = (v: number | null | undefined, rounded?: 'rounded') =>
  typeof v === 'number' && Number.isFinite(v)
    ? format(v >= 1000 && rounded ? '.3s' : ',.0f')(v)
    : '—'
