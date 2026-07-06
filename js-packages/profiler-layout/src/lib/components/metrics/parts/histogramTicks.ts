/**
 * "Nice" tick values in [0, max] using a 1-2-5-10 step, at most `maxCount` of them (0 included).
 * Values are round multiples (…, 500, 1000, 2000, 5000, …) for easy reading; the caller positions
 * them on whatever scale it draws (e.g. a log axis). With `integer` the step is forced to a whole
 * number, so bin/step counts never get fractional ticks. Returns [] for a non-positive max.
 */
export function niceTicks(max: number, maxCount = 5, integer = false): number[] {
  if (!(max > 0) || maxCount < 2) {
    return []
  }
  const niceStep = (raw: number): number => {
    const mag = 10 ** Math.floor(Math.log10(raw))
    const n = raw / mag
    return (n < 1.5 ? 1 : n < 3 ? 2 : n < 7 ? 5 : 10) * mag
  }
  // Next nice step up (1 → 2 → 5 → 10 …), used to thin the ticks when there are too many.
  const nextStep = (s: number): number => {
    const mag = 10 ** Math.floor(Math.log10(s))
    const n = Math.round(s / mag)
    return (n < 2 ? 2 : n < 5 ? 5 : 10) * mag
  }
  const build = (step: number): number[] => {
    const out: number[] = []
    for (let v = 0; v <= max + step * 1e-9; v += step) {
      out.push(Number(v.toFixed(10)))
    }
    return out
  }
  let step = niceStep(max / (maxCount - 1))
  if (integer) {
    step = Math.max(1, step)
  }
  let ticks = build(step)
  while (ticks.length > maxCount) {
    step = nextStep(step)
    ticks = build(step)
  }
  return ticks
}
