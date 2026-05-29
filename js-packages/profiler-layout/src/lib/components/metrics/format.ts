// Formatting helpers for the metrics view.
//
// profiler-lib hands us only pre-formatted display strings (`TooltipCell.value`) — there is no
// raw number or unit on the cell. The bar chart, however, needs numbers to compute spread,
// colors, and log-scaled heights. So we parse the display string back into a number
// (`parseFormatted`) and re-format it for the avg/min/max columns (`formatNumber`). This
// round-trip is a known compromise; the cleaner fix is for profiler-lib to expose the raw
// values and units, at which point these helpers can be retired.
//
// These `Format`s cover the numeric metrics the bar chart renders. Non-numeric metrics (plain
// strings, booleans) are shown verbatim from `TooltipCell.value` elsewhere and never reach
// these helpers — `parseFormatted` returns NaN for them and they are not charted.
export type Format = 'time' | 'qty' | 'bytes' | 'percent'

function formatScaled(value: number, base: number, prefixes: string[]): string {
  let abs = Math.abs(value)
  let index = 0
  while (abs >= base && index < prefixes.length - 1) {
    abs = abs / base
    value = value / base
    index++
  }
  return value.toLocaleString('en-US', { maximumFractionDigits: 2 }) + prefixes[index]
}

export function formatNumber(value: number, format: Format): string {
  if (!Number.isFinite(value)) {
    return 'N/A'
  }
  switch (format) {
    case 'qty':
      return formatScaled(value, 1000, ['', 'K', 'M', 'B'])
    case 'bytes':
      return formatScaled(value, 1024, [' B', ' KiB', ' MiB', ' GiB', ' TiB'])
    case 'percent':
      return value.toFixed(1) + '%'
    case 'time':
      if (value === 0) {
        return '0s'
      }
      if (value < 0.001) {
        return (value * 1_000_000).toLocaleString('en-US', { maximumFractionDigits: 2 }) + 'us'
      }
      if (value < 1) {
        return (value * 1000).toLocaleString('en-US', { maximumFractionDigits: 2 }) + 'ms'
      }
      return value.toLocaleString('en-US', { maximumFractionDigits: 2 }) + 's'
  }
}

/** Parse a pre-formatted profiler-lib value string back into a raw number, for math (skew, color
 *  interpolation, log-scaling). Returns NaN if we can't parse it. */
export function parseFormatted(formatted: string): number {
  const s = formatted.trim()
  if (s === 'N/A' || s === '') {
    return NaN
  }
  // Handle percent
  if (s.endsWith('%')) {
    return parseFloat(s.slice(0, -1))
  }
  // Handle time units (us, ms, s)
  const time = s.match(/^(-?[\d,.]+)\s*(us|ms|s)$/i)
  if (time) {
    const n = parseFloat(time[1]!.replace(/,/g, ''))
    if (time[2] === 'us') {
      return n / 1_000_000
    }
    if (time[2] === 'ms') {
      return n / 1000
    }
    return n
  }
  // Handle byte units
  const bytes = s.match(/^(-?[\d,.]+)\s*(B|KiB|MiB|GiB|TiB)$/)
  if (bytes) {
    const n = parseFloat(bytes[1]!.replace(/,/g, ''))
    const exp = ['B', 'KiB', 'MiB', 'GiB', 'TiB'].indexOf(bytes[2]!)
    return n * 1024 ** exp
  }
  // Handle K/M/B suffixes (count)
  const count = s.match(/^(-?[\d,.]+)\s*(K|M|B)?$/)
  if (count) {
    const n = parseFloat(count[1]!.replace(/,/g, ''))
    const suffix = count[2]
    if (suffix === 'K') {
      return n * 1_000
    }
    if (suffix === 'M') {
      return n * 1_000_000
    }
    if (suffix === 'B') {
      return n * 1_000_000_000
    }
    return n
  }
  return NaN
}

/** Heuristically infer a Format from a pre-formatted profiler-lib value. */
export function inferFormat(formatted: string): Format {
  const s = formatted.trim()
  if (s.endsWith('%')) {
    return 'percent'
  }
  if (/(?:^|\s)(us|ms|s)$/i.test(s)) {
    return 'time'
  }
  if (/(?:B|KiB|MiB|GiB|TiB)$/.test(s)) {
    return 'bytes'
  }
  return 'qty'
}
