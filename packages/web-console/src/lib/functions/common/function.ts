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

/**
 * Create a closure with an argument value
 * Useful for fine-grained reactivity in Svelte 5
 * The contents of the closure do not get picked up by Svelte 5's deep reactivity
 */
export const enclosure = <T>(v: T) => {
  let state = v
  return () => state
}

/**
 * Get return value from a closure and return a new closure containing that value
 * Useful for fine-grained reactivity in Svelte 5
 * It allows triggering the reactive update on a value stored in a closure by "repackaging" contents of the closure and re-assigning them
 */
export const reclosure = <T>(f: () => T) => {
  const v = f()
  return () => v
}

/**
 * Reassign a closure in the key of an object
 * @see reclosure
 */
export const reclosureKey = <K extends keyof any, T extends { [k in K]: () => unknown }>(
  t: T,
  k: K
) => {
  t[k] = reclosure(t[k]) as T[K]
}
