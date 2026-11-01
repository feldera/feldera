import { format } from 'd3-format'
import Dayjs from 'dayjs'
import type { Microseconds } from './duration.ts'

export const formatDateTime = (
  timestamp: Date | Dayjs.Dayjs | { ms: number },
  format = 'MMM D, YYYY h:mm A'
) => {
  return Dayjs('ms' in timestamp ? timestamp.ms : timestamp).format(format)
}

const relativeTime = new Intl.RelativeTimeFormat('en', { style: 'long', numeric: 'always' })

/**
 * How long ago `timestamp` was, in its single largest unit — `just now`, `34 minutes ago`,
 * `9 hours ago`, `2 days ago`, `3 weeks ago`, `2 months ago`, `1 year ago`. Every age reads
 * the same way so that a column of them can be compared at a glance. Units are floored, so
 * a label never claims more time has passed than has.
 */
export const formatTimeAgo = (timestamp: Date, now: Date) => {
  // A timestamp ahead of the clock ages to nothing rather than to a negative duration.
  const elapsed = Math.max(now.valueOf() - timestamp.valueOf(), 0)
  const minutes = Math.floor(elapsed / 60_000)
  const hours = Math.floor(minutes / 60)
  const days = Math.floor(hours / 24)
  if (minutes < 1) {
    return 'just now'
  }
  if (hours < 1) {
    return relativeTime.format(-minutes, 'minute')
  }
  if (days < 1) {
    return relativeTime.format(-hours, 'hour')
  }
  if (days < 7) {
    return relativeTime.format(-days, 'day')
  }
  if (days < 30) {
    return relativeTime.format(-Math.floor(days / 7), 'week')
  }
  // Months and years are calendar ones; 30 days is not always a whole month, but the
  // label still never drops below one.
  const months = Math.max(Dayjs(now).diff(timestamp, 'month'), 1)
  if (months < 12) {
    return relativeTime.format(-months, 'month')
  }
  return relativeTime.format(-Math.floor(months / 12), 'year')
}

/**
 * Format the time of day of a timestamp in the browser's time zone, followed by
 * the zone's short name (e.g. `18:39:53 PDT`), so it's clear which zone it's in.
 */
export const formatServerTime = (time: Date | Dayjs.Dayjs | string) => {
  const date = Dayjs(time).toDate()
  const zone = new Intl.DateTimeFormat(undefined, { timeZoneName: 'short' })
    .formatToParts(date)
    .find((part) => part.type === 'timeZoneName')?.value
  return [Dayjs(date).format('HH:mm:ss'), zone].filter(Boolean).join(' ')
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
