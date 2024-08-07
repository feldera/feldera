/**
 * Return arguments as-is with type inferred as a tuple
 * @example
 * [3, 'three', true]: (string | number | boolean)[]
 * tuple(3, 'three', true): [number, string, boolean]
 * @param t Arguments to be passed down as a tuple
 * @returns
 */
export function tuple<T extends unknown[]>(...t: [...T]) {
  return t
}

/**
 * Return a list of argument tuples as-is with type element type inferred as a tuple.
 * All elements should have the same type
 * @example
 * [[3, 'three', true], [4, 'four', false]]: (string | number | boolean)[][]
 * tuples([3, 'three', true], [4, 'four', false]): [number, string, boolean][]
 * @param t List of arguments to be passed down as tuples
 * @returns
 */
export function tuples<T extends unknown[]>(...t: [...T][]) {
  return t
}

/**
 * Zip lists together.
 * @see https://stackoverflow.com/a/70192772
 * @example
 * zip([1,2,3], ["a","b","c","d"]) // [[1, "a"], [2, "b"], [3, "c"], [undefined, "d"]]
 * @param arr Arrays to zip
 * @returns
 */
export function zip<T extends unknown[][]>(
  ...args: T
): { [K in keyof T]: T[K] extends (infer V)[] ? V : never }[] {
  const minLength = Math.min(...args.map((arr) => arr.length))
  // @ts-expect-error This is too much for ts
  return Array(minLength)
    .fill(undefined)
    .map((_, i) => args.map((arr) => arr[i]))
}

/**
 * Unzip a list of similar tuples into separate lists.
 * @see https://stackoverflow.com/a/72650077
 * @param arr A list of tuples to unzip into lists
 * @returns
 */
export function unzip<T extends [...{ [K in keyof S]: S[K] }][], S extends any[]>(
  arr: [...T]
): T[0] extends infer A ? { [K in keyof A]: T[number][K & keyof T[number]][] } : never {
  const maxLength = Math.max(...arr.map((x) => x.length))

  return arr.reduce((acc: any, val) => {
    val.forEach((v, i) => acc[i].push(v))
    return acc
  }, Array(maxLength).fill([]))
}

/**
 * Zip two lists together.
 * If lists have different length - the missing elements are replaced with default values.
 * All nullish elements get replaced with default values
 * @example
 * zipDefault<number | undefined, string | undefined>
 *   (undefined, undefined)([1,2,3], ["a","b","c","d"])
 * // [[1, "a"], [2, "b"], [3, "c"], [undefined, "d"]]
 * @param dA
 * @param dB
 * @returns
 */
export const zipDefault = <A, B>(dA: A, dB: B) => zipWithDefault((a, b) => tuple(a, b), dA, dB)

/**
 * Zip two lists together with a custom function.
 * If lists have different length - the missing elements are replaced with default values.
 * All nullish elements get replaced with default values
 * @example
 * zipWithDefault((a, b) => a + b, 0, 0)([1,2,3], [10, 20])
 * // [11, 22, 3]
 * @param dA
 * @param dB
 * @returns
 */
export const zipWithDefault =
  <A, B, R>(f: (a: A, b: B) => R, dA: A, dB: B) =>
  (as: A[], bs: B[]) =>
    Array.from(Array(Math.max(as.length, bs.length)), (_, i) => f(as[i] ?? dA, bs[i] ?? dB))
