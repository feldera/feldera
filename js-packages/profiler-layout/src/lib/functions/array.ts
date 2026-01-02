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
