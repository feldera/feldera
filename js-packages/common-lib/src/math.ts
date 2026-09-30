/**
 * Clamp a `number` between `min` and `max`.
 * The min and max are inclusive.
 * @param number
 * @param min
 * @param max
 * @returns
 */
export function clamp(number: number, min: number, max: number) {
  return Math.max(min, Math.min(number, max))
}

/**
 * Apply a function to pairs of elements.
 * @param arr
 * @param f
 * @returns the list of results shorter by 1
 */
export const discreteDerivative = <T, R>(arr: T[], f: (n1: T, n0: T, i: number, arr: T[]) => R) => {
  if (!arr.length) {
    return []
  }
  const len = arr.length - 1,
    result = new Array<R>(len)
  for (let i = 0; i < len; ++i) {
    result[i] = f(arr[i + 1], arr[i], i + 1, arr)
  }
  return result
}
