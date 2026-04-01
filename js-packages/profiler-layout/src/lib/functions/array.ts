/**
 * Group values using a key function.
 * This implementation is stable: it preserves the
 * order of the groups and of the elements within each group.
 */
export const groupBy = <T, K extends string | number>(list: T[], getKey: (item: T) => K) => {
  if (!list.length) {
    return []
  }
  const map = new Map<K, T[]>()
  for (const item of list) {
    const key = getKey(item)
    let bucket = map.get(key)
    if (bucket) {
      bucket.push(item)
    } else {
      map.set(key, [item])
    }
  }
  // JavaScript promises that this is stable: entries are read
  // in the order of insertion.
  return Array.from(map.entries())
}
