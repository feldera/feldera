/**
 * Group elements into two based on a binary predicate
 * @param arr
 * @param predicate
 * @returns [is true, is false]
 */
export const partition = <T>(arr: T[], predicate: (v: T, i: number, ar: T[]) => boolean) =>
  arr.reduce(
    (acc, item, index, array) => {
      acc[+!predicate(item, index, array)].push(item)
      return acc
    },
    [[], []] as [T[], T[]]
  )

export function inUnion<T extends readonly string[]>(union: T, val: string | unknown): val is T[number] {
  return typeof val === 'string' && union.includes(val)
}

export function invariantUnion<T extends readonly string[]>(union: T, val: string): asserts val is T[number] {
  if (!union.includes(val)) {
    throw new Error(val + ' is not part of the union ' + union.toString())
  }
}

export function assertUnion<T extends readonly string[]>(union: T, val: string): T[number] {
  if (!union.includes(val)) {
    throw new Error(val + ' is not part of the union ' + union.toString())
  }
  return val
}

/**
 * Mutates the array
 * @param array
 * @param fromIndex
 * @param toIndex
 * @returns
 */
export function reorderElement<T>(array: T[], fromIndex: number, toIndex: number) {
  if (fromIndex === toIndex || fromIndex < 0 || toIndex < 0 || fromIndex >= array.length || toIndex >= array.length) {
    // No need to move if the indices are the same or out of bounds.
    return array
  }

  const [movedElement] = array.splice(fromIndex, 1)

  array.splice(toIndex, 0, movedElement)

  return array
}

/**
 * Replaces the first element in the array for which the replacement result isn't null
 * Mutates the array
 * @param array
 * @param replacement
 * @returns
 */
export function replaceElementInplace<T>(array: T[], replacement: (t: T) => T | null) {
  let value = null as T | null
  for (const [i, e] of array.entries()) {
    value = replacement(e)
    if (value !== null) {
      array[i] = value
      break
    }
  }
  return array
}

/**
 * Replaces the first element in the array for which the replacement result isn't null
 * Returns a new array
 * @param array
 * @param replacement
 * @returns
 */
export function replaceElement<T>(array: T[], replacement: (t: T) => T | null) {
  let value = null as T | null
  for (const [i, e] of array.entries()) {
    value = replacement(e)
    if (value !== null) {
      return array.slice(0, i).concat([value], array.slice(i + 1))
    }
  }
  return array
}

/**
 * Returns a new array with a separator between each element in the source array
 * Behaves similarly to String.join
 * @param arr
 * @param separator
 * @returns
 */
export const intersperse = <T>(arr: T[], separator: T | ((i: number) => T)) => {
  const getSeparator = separator instanceof Function ? separator : () => separator
  return Array.from({ length: Math.max(0, arr.length * 2 - 1) }, (_, i) => (i % 2 ? getSeparator(i) : arr[i >> 1]))
}

export const groupBy = <T, K extends keyof any>(list: T[], getKey: (item: T) => K) => {
  if (!list.length) {
    return []
  }
  const items = list.map(item => [getKey(item), item] as [K, T])
  items.sort()
  let lastKeyIndex = 0
  const groups = [] as [K, T[]][]
  while (lastKeyIndex < list.length) {
    const lastIndex = items.findLastIndex(item => item[0] === items[lastKeyIndex][0]) + 1
    groups.push([items[lastKeyIndex][0], items.slice(lastKeyIndex, lastIndex).map(item => item[1])])
    lastKeyIndex = lastIndex
  }
  return groups
}
