import { nonNull } from '$lib/functions/common/function'

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

export function inUnion<T extends readonly string[]>(
  union: T,
  val: string | unknown
): val is T[number] {
  return typeof val === 'string' && union.includes(val)
}

export function invariantUnion<T extends readonly string[]>(
  union: T,
  val: string
): asserts val is T[number] {
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
  if (
    fromIndex === toIndex ||
    fromIndex < 0 ||
    toIndex < 0 ||
    fromIndex >= array.length ||
    toIndex >= array.length
  ) {
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
  return Array.from({ length: Math.max(0, arr.length * 2 - 1) }, (_, i) =>
    i % 2 ? getSeparator(i) : arr[i >> 1]
  )
}

const compare = <T extends string | number>(a: T, b: T) =>
  typeof a === 'string'
    ? a.localeCompare(b as string)
    : typeof a === 'number'
      ? a - (b as number)
      : (() => {
          throw new Error('Cannot compare unsupported types')
        })()

/**
 * The groups are ordered by associated key. Order of items in a group is arbitrary
 */
export const groupBy = <T, K extends string | number>(list: T[], getKey: (item: T) => K) => {
  if (!list.length) {
    return []
  }
  const items = list.map((item) => [getKey(item), item] as [K, T])
  items.sort((a, b) => compare(a[0], b[0]))
  let lastKeyIndex = 0
  const groups = [] as [K, T[]][]
  while (lastKeyIndex < list.length) {
    const lastIndex = items.findLastIndex((item) => item[0] === items[lastKeyIndex][0]) + 1
    groups.push([
      items[lastKeyIndex][0],
      items.slice(lastKeyIndex, lastIndex).map((item) => item[1])
    ])
    lastKeyIndex = lastIndex
  }
  return groups
}

/**
 * Return an array containing only unique elements from the original array in the same order.
 * Keep the first occurrence of the element in the array
 */
export const nubLast = <T>(array: T[], getKey = (v: T): string | number => v as any) =>
  array.filter((value, index, arr) => {
    const key = getKey(value)
    return arr.findIndex((item) => getKey(item) === key) === index
  })

/**
 * Return an array containing only unique elements from the original array in the same order.
 * Keep the last occurrence of the element in the array
 */
export const nubFirst = <T>(array: T[], getKey = (v: T): string | number => v as any) =>
  array.filter((value, index, arr) => {
    const key = getKey(value)
    return arr.findLastIndex((item) => getKey(item) === key) === index
  })

/**
 * Performs no checks on uniqueness of inputs
 */
export const intersect2 = <A, B, C, Key extends number | string>(
  as: A[],
  bs: B[],
  getKeyA: (a: A) => Key,
  getKeyB: (b: B) => Key,
  zip: (a: A, b: B) => C
) => {
  return as.flatMap((a) => {
    const keyA = getKeyA(a)
    const match = bs.findIndex((b) => keyA === getKeyB(b))
    return match === -1 ? [] : [zip(a, bs[match])]
  })
}

/**
 * Produce a list of indices that would split a sequence in chunks of a maximum size
 */
export function chunkIndices(min: number, max: number, size: number): number[] {
  const result: number[] = []
  for (let i = min; i < max; i += size) {
    result.push(i)
  }
  result.push(max)
  return result
}

export function count<T>(arr: T[], pred: (v: T) => boolean | number | null | undefined) {
  let count = 0
  for (const e of arr) {
    const result = pred(e)
    if (typeof result === 'number') {
      count += result
    } else if (result) {
      ++count
    }
  }
  return count
}

export function findIndex<T, S>(
  arr: T[],
  predicate: (state: S, value: T, index: number, obj: T[]) => [boolean, S],
  initialState: S
) {
  let state = initialState
  return arr.findIndex((value, index, obj) => {
    let res: boolean
    ;[res, state] = predicate(state, value, index, obj)
    return res
  })
}

/**
 * @param arr Numbers sorted in ascending order
 * @param target
 * @returns -1 if value is not found
 */
export function binarySearchNumber(arr: number[], target: number) {
  let left = 0
  let right = arr.length - 1

  while (left <= right) {
    const mid = Math.floor((left + right) / 2)

    if (arr[mid] === target) {
    }
    if (arr[mid] < target) {
      left = mid + 1
    } else if (arr[mid] === target) {
      return mid
    } else {
      right = mid - 1
    }
  }

  return -1
}

/**
 * Finds the index of the smallest element in a sorted array that is greater-than-or-equal to the target.
 * If no such element exists (i.e., all elements are less than the target), returns -1.
 *
 * @param arr - A sorted array of numbers in ascending order
 * @param target - The target number to search for
 * @returns The index of the minimum value in arr that is >= target, or -1 if no such value exists.
 */
export function binarySearchMin(arr: number[], target: number): number {
  let left = 0
  let right = arr.length - 1
  let result = -1

  while (left <= right) {
    const mid = Math.floor((left + right) / 2)

    if (arr[mid] >= target) {
      result = mid // We found a candidate, move left to find a smaller valid index
      right = mid - 1
    } else {
      left = mid + 1 // Mid element is too small, move right
    }
  }

  return result
}

/**
 * Finds the index of the largest element in a sorted array that is less-than-or-equal to the target.
 * If no such element exists (i.e., all elements are greater than the target), returns -1.
 *
 * @param arr - A sorted array of numbers in ascending order
 * @param target - The target number to search for
 * @returns The index of the maximum value in arr that is <= target, or -1 if no such value exists.
 */
export function binarySearchMax(arr: number[], target: number): number {
  let left = 0
  let right = arr.length - 1
  let result = -1

  while (left <= right) {
    const mid = Math.floor((left + right) / 2)

    if (arr[mid] <= target) {
      result = mid // We found a candidate, move right to find a larger valid index
      left = mid + 1
    } else {
      right = mid - 1 // Mid element is too large, move left
    }
  }

  return result
}

export const singleton = <T>(value: T | null | undefined) => (nonNull(value) ? [value] : [])

/**
 * Finds the first element matching the predicate in `arr`, removes it,
 * and optionally inserts the provided items at that same index.
 *
 * @param arr          — The array to mutate.
 * @param predicate    — Called with (element, index, array); return true to match.
 * @param insertItems? — If provided, these items will be inserted at the removal index.
 * @returns The removed element, or `undefined` if no match was found.
 */
export function findSplice<T>(
  arr: T[],
  predicate: (element: T, index: number, array: T[]) => boolean,
  ...insertItems: T[]
): [T] | undefined {
  const idx = arr.findIndex(predicate)
  if (idx === -1) return undefined

  // splice: at idx, remove exactly 1 element, then insert any insertItems
  return arr.splice(idx, 1, ...insertItems) as [T]
}
