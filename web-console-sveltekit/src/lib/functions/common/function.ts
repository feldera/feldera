/**
 *
 * @param value
 * @returns
 */
export function nonNull<T>(value: T | null | undefined): value is NonNullable<T> {
  return value !== null && value !== undefined
}

export const compose =
  <Args extends unknown[], R1, R2>(f: (...args: Args) => R1, g: (r: R1) => R2) =>
  (...args: Args) =>
    g(f(...args))
