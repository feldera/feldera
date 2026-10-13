/**
 * Color scale for the input connectors' latency column.
 *
 * The scale spans the connectors that currently report a latency: the fastest
 * keeps the default text color, the slowest turns error red, and the rest land
 * on a blend between the two.
 */

import invariant from 'tiny-invariant'

/**
 * Lower bound on the scale's spread, as a multiple of the fastest connector's
 * latency: the top of the scale never sits below `min * spread`.
 *
 * At the default of 5 - a connector must be at least
 * five times slower than the fastest one to saturate its color.
 */
export const defaultLatencyColorSpread = 5

export type LatencyColorScale = {
  /** Fastest connector's latency in microseconds; drawn in the base color. */
  min: number
  /** Latency in microseconds at which the color saturates. */
  max: number
}

const finiteLatencies = (latencies: ReadonlyArray<number | null | undefined>) =>
  latencies.filter(
    (latency): latency is number =>
      typeof latency === 'number' && Number.isFinite(latency) && latency >= 0
  )

/**
 * Builds the scale from every connector's latency, skipping connectors without
 * samples. Returns `undefined` when no connector reports one, which callers
 * render as uncolored text.
 */
export const latencyColorScale = (
  latencies: ReadonlyArray<number | null | undefined>,
  spread: number = defaultLatencyColorSpread
): LatencyColorScale | undefined => {
  invariant(spread >= 1, 'Minimum latency spread cannot be less than 1')
  const values = finiteLatencies(latencies)
  if (values.length === 0) {
    return undefined
  }
  const min = Math.min(...values)
  const observedMax = Math.max(...values)
  return { min, max: Math.max(observedMax, min * spread) }
}

/**
 * Position of `latency` on `scale`, clamped to `[0, 1]`.
 */
export const latencyColorFraction = (
  latency: number | null | undefined,
  scale: LatencyColorScale | undefined
): number => {
  if (!scale || typeof latency !== 'number' || !Number.isFinite(latency)) {
    return 0
  }
  const span = scale.max - scale.min
  if (span <= 0) {
    return 0
  }
  return Math.min(1, Math.max(0, (latency - scale.min) / span))
}

/**
 * CSS color for `latency`: the `--latency-fast` token at the bottom of the
 * scale, `--latency-slow` at the top.
 *
 * The consuming component defines both tokens as single-tone Skeleton colors
 * with a `.dark` override. Skeleton's dual-tone `*-950-50` variables cannot be
 * used directly, because their nested `light-dark()` resolves to `transparent`
 * inside `color-mix()`.
 */
export const latencyColor = (
  latency: number | null | undefined,
  scale: LatencyColorScale | undefined
): string => {
  const slow = latencyColorFraction(latency, scale) * 100
  return `color-mix(in oklab, var(--latency-fast) ${(100 - slow).toFixed(2)}%, var(--latency-slow) ${slow.toFixed(2)}%)`
}
