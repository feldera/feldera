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

export function inUnion<T extends readonly string[]>(union: T, val: string): val is T[number] {
  return union.includes(val)
}
