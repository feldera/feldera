import { createSubscriber } from 'svelte/reactivity'
import { type BundleHistoryEntry, observeBundleHistory } from '$lib/services/supportBundleHistory'

/**
 * The support bundle history, as state a component can read.
 *
 * Dexie runs the query behind `observeBundleHistory` again after every change to the
 * database, in this tab and in the others, so a component that reads `current` is
 * never told that a bundle was added, opened or forgotten. It just has the new list.
 *
 * The subscription starts when the first effect reads `current`, and ends when the
 * last such effect is destroyed. Read outside an effect, `current` is whatever the
 * last subscription left behind, and an empty list before the first one.
 */
let entries = $state<BundleHistoryEntry[]>([])

const subscribe = createSubscriber(() => {
  const subscription = observeBundleHistory().subscribe(
    (next) => {
      entries = next
    },
    (error) => {
      // A history that cannot be read is an empty one. It is a convenience, and
      // losing it must not stop the user opening a bundle from disk.
      console.warn('Failed to read the support bundle history:', error)
      entries = []
    }
  )
  return () => subscription.unsubscribe()
})

export const useSupportBundleHistory = () => ({
  get current() {
    subscribe()
    return entries
  }
})
