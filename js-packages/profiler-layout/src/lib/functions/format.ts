/**
 * Convert a byte count to a human-readable string, e.g. humanSize(1024) === "1.0 KiB".
 * Ported from web-console's string utils so the profiler view can format byte axes/labels.
 */
export function humanSize(bytes: number): string {
  const thresh = 1024
  if (Math.abs(bytes) < thresh) {
    return bytes + ' B'
  }
  const units = ['KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB']
  let u = -1
  do {
    bytes /= thresh
    ++u
  } while (Math.abs(bytes) >= thresh && u < units.length - 1)
  return bytes.toFixed(1) + ' ' + units[u]
}

/** Format a duration in seconds as a compact human-readable string (ns/µs/ms/s). */
export function humanSeconds(s: number): string {
  if (!Number.isFinite(s)) {
    return 'n/a'
  }
  if (s === 0) {
    return '0'
  }
  const abs = Math.abs(s)
  if (abs < 1e-6) {
    return (s * 1e9).toFixed(0) + 'ns'
  }
  if (abs < 1e-3) {
    return (s * 1e6).toFixed(1) + 'µs'
  }
  if (abs < 1) {
    return (s * 1e3).toFixed(1) + 'ms'
  }
  return s.toFixed(2) + 's'
}

/**
 * Resolve a CSS custom property (e.g. '--color-primary-500') to its computed value. ECharts
 * renders to canvas and cannot consume CSS variables, so charts read theme colors this way.
 * Returns `fallback` when unavailable (e.g. SSR, or the variable is unset).
 */
export function cssVar(name: string, fallback: string): string {
  if (typeof document === 'undefined') {
    return fallback
  }
  const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim()
  return v || fallback
}

export type Rgb = [number, number, number]

let colorProbe: HTMLSpanElement | undefined
let normCtx: CanvasRenderingContext2D | null | undefined

/**
 * Resolve any CSS color expression (hex, `oklch(…)`, `var(--…)`) to concrete sRGB `[r, g, b]`.
 * `getComputedStyle` resolves the `var()`, then a 1×1 canvas pixel converts the (possibly
 * `oklch`) computed color to sRGB — parsing the computed string directly would misread oklch's
 * L/C/H as R/G/B. ECharts renders to canvas and cannot interpolate CSS colors, so charts resolve
 * endpoints to RGB and interpolate in JS. Returns `fallback` outside the browser.
 */
export function resolveRgb(expr: string, fallback: Rgb): Rgb {
  if (typeof document === 'undefined') {
    return fallback
  }
  if (!colorProbe) {
    colorProbe = document.createElement('span')
    colorProbe.style.display = 'none'
    document.body.appendChild(colorProbe)
  }
  colorProbe.style.color = ''
  colorProbe.style.color = expr
  const computed = getComputedStyle(colorProbe).color
  if (!computed) {
    return fallback
  }
  if (normCtx === undefined) {
    const canvas = document.createElement('canvas')
    canvas.width = 1
    canvas.height = 1
    normCtx = canvas.getContext('2d', { willReadFrequently: true })
  }
  if (!normCtx) {
    return fallback
  }
  normCtx.clearRect(0, 0, 1, 1)
  try {
    normCtx.fillStyle = computed
  } catch {
    return fallback
  }
  normCtx.fillRect(0, 0, 1, 1)
  const d = normCtx.getImageData(0, 0, 1, 1).data
  return [d[0]!, d[1]!, d[2]!]
}

/** Resolve a CSS color expression to an `rgb(...)` string (for ECharts axis/series colors). */
export function resolveCssColor(expr: string, fallback: Rgb): string {
  const [r, g, b] = resolveRgb(expr, fallback)
  return `rgb(${r}, ${g}, ${b})`
}

/** True when the app is in dark mode (the metrics theme keys off `.dark` / `body.dark`). */
export function isDarkTheme(): boolean {
  if (typeof document === 'undefined') {
    return false
  }
  return (
    document.documentElement.classList.contains('dark') || document.body.classList.contains('dark')
  )
}

/** Linearly interpolate between two RGB colors, `t` clamped to [0, 1]; returns `rgb(...)`. */
export function mixRgb(a: Rgb, b: Rgb, t: number): string {
  const k = Math.max(0, Math.min(1, Number.isFinite(t) ? t : 0))
  const r = Math.round(a[0] + (b[0] - a[0]) * k)
  const g = Math.round(a[1] + (b[1] - a[1]) * k)
  const bl = Math.round(a[2] + (b[2] - a[2]) * k)
  return `rgb(${r}, ${g}, ${bl})`
}

/**
 * The shared cache-diagram color scale endpoints: a theme-adaptive neutral (surface-100 in
 * light mode, surface-900 in dark — subtle against the card) and error. Used for the value
 * heatmap and for corner-based dot coloring so both read on the same scale.
 */
export function neutralErrorScale(): { neutral: Rgb; error: Rgb } {
  const neutral = resolveRgb(
    isDarkTheme() ? 'var(--color-surface-900)' : 'var(--color-surface-100)',
    isDarkTheme() ? [30, 33, 40] : [241, 243, 246]
  )
  const error = resolveRgb('var(--color-error-500)', [239, 68, 68])
  return { neutral, error }
}
