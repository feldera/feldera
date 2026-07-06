import { format } from 'd3-format'

/**
 * Format a quantity (record/row count) for display.
 * Copied verbatim from web-console's `$lib/functions/format` so the profiler view can format
 * count axes/labels identically. Kept as a standalone copy on purpose: the web-console impl is
 * not shared, so the two stay in lockstep by duplication rather than a cross-package import.
 *
 *  - default: comma-grouped integer (`,.0f`), e.g. 12345 -> "12,345".
 *  - `'rounded'`: 3 significant digits with an SI suffix (`.3s`) once >= 1000, e.g. "12.3k".
 */
export const formatQty = (v: number | null | undefined, rounded?: 'rounded') =>
  typeof v === 'number' && Number.isFinite(v)
    ? format(v >= 1000 && rounded ? '.3s' : ',.0f')(v)
    : '—'
