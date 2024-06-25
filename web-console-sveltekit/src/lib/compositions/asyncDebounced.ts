import { asyncWritable, type WritableLoadable } from "@square/svelte-store";

import { useDebounce } from "runed";

export const asyncDebounced = <T>(store: WritableLoadable<T>) => {
  const debounce = useDebounce((s: T) => {
    store.set(s)
  }, 1000)
  return asyncWritable(
    store,
    s => s,
    async s => {
      debounce(s)
      return s
    }
  )
}