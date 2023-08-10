// Clamps a `number` between `min` and `max`.
//
// The min and max are inclusive.
export function clamp(number: number, min: number, max: number) {
  return Math.max(min, Math.min(number, max))
}
