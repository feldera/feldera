import { asyncWritable, type WritableLoadable } from '@square/svelte-store'

import { useDebounce } from 'runed'

export const asyncDebounced = <T>(store: WritableLoadable<T>, wait: number | (() => number)) => {
  const debounce = useDebounce((s: T) => {
    store.set(s)
  }, wait)
  debounce
  return asyncWritable(
    store,
    (s) => s,
    async (s) => {
      debounce(s)
      return s
    }
  )
}
