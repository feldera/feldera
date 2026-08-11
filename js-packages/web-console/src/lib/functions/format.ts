import { format } from 'd3-format'
import Dayjs from 'dayjs'
import type { Microseconds } from '$lib/functions/common/duration'

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

const trimZeros = (s: string) => (s.includes('.') ? s.replace(/\.?0+$/, '') : s)

/**
 * Format a duration using an adaptive unit (µs / ms / s) with roughly three
 * significant figures. Zero is rendered bare, without a unit.
 *
 *   formatDuration(microseconds(0))         -> "0"
 *   formatDuration(microseconds(340))       -> "340 µs"
 *   formatDuration(microseconds(1_200))     -> "1.2 ms"
 *   formatDuration(microseconds(2_100_000)) -> "2.1 s"
 */
export const formatDuration = (micros: Microseconds | null | undefined): string => {
  if (typeof micros !== 'number' || !Number.isFinite(micros)) {
    return '—'
  }
  if (micros === 0) {
    return '0'
  }
  const [value, unit] =
    micros < 1_000
      ? [micros, 'µs']
      : micros < 1_000_000
        ? [micros / 1_000, 'ms']
        : [micros / 1_000_000, 's']
  const digits = value >= 100 ? 0 : value >= 10 ? 1 : 2
  return `${trimZeros(value.toFixed(digits))} ${unit}`
}
