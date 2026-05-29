// Color helpers for the metrics view.
// We rely on CSS color-mix() so the resulting colors honor Skeleton's light/dark theme tokens
// (the *-100-900 variables resolve to surface-100 in light mode and surface-900 in dark mode).

function clamp01(t: number): number {
  if (!Number.isFinite(t)) {
    return 0
  }
  return Math.max(0, Math.min(1, t))
}

/**
 * Mix two theme colors by fraction `t` (clamped to [0, 1]) and return a CSS
 * `color-mix()` expression. `low` and `high` are the *names* of CSS custom
 * properties (e.g. '--bar-low', '--bar-high'); the result is `low` at t=0, `high`
 * at t=1, and a blend in between. Returning an expression (rather than a
 * resolved color) lets the browser re-evaluate it whenever the underlying
 * theme tokens change — that's how light/dark switching works without JS.
 */
export function lerpThemeColor(t: number, low: string, high: string): string {
  const p = clamp01(t) * 100
  return `color-mix(in oklab, var(${low}) ${(100 - p).toFixed(2)}%, var(${high}) ${p.toFixed(2)}%)`
}

/** Bar color: floor (low) → ceiling (high). Consumed by BarChartMetric.svelte (which passes
 *  each bar's normalized height `t`). Uses the `--bar-low`/`--bar-high` CSS variables defined in
 *  MetricsDistributionBlock.svelte's style block, which bind to single-tone Skeleton tokens with
 *  a `.dark` override. We avoid Skeleton's dual-tone `*-200-800` vars here because nested
 *  `light-dark()` resolution inside `color-mix()` resolves to `transparent` in some setups
 *  (cascade re-parsing of the dual-tone value doesn't reach the data-theme scope where the
 *  singular tones are defined). */
export function barColor(t: number): string {
  return lerpThemeColor(t, '--bar-low', '--bar-high')
}

/** Skew text color: neutral up to ~0%, then interpolates to error. 50% skew saturates. */
export function skewTextColor(skewPct: number): string {
  if (!Number.isFinite(skewPct) || skewPct <= 0) {
    return 'var(--skew-low)'
  }
  const t = clamp01(skewPct / 50)
  return lerpThemeColor(t, '--skew-low', '--skew-high')
}

/** Normalized log curve: f(0)=0, f(1)=1, concave (emphasizes small values). */
export function logScale01(t: number): number {
  const x = clamp01(t)
  // log(1 + x*(e-1)) / log(e) === log(1 + x*(e-1))
  return Math.log(1 + x * (Math.E - 1))
}
