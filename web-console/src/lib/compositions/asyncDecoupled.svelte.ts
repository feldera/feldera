import {
  get,
  writable,
  type Readable,
  type Subscriber,
  type WritableLoadable
} from '@square/svelte-store'

import { useDebounce } from 'runed'

// import { useDebounce } from "$lib/compositions/debounce.svelte";

/**
 * Upstream and downstream changes do not immediately affect each other
 * On `.pull()` upstream is applied to downstream
 * On `.push()` downstream is applied to upstream
 */
export const asyncDecoupled = <T>(store: WritableLoadable<T>, wait: () => number | 'decoupled') => {
  let upstreamChanged = $state(false)
  let downstreamChanged = $state(false)
  const state = writable(get(store))
  const shouldWait = writable({ wait, last: wait })
  // const debounceSet = useDebounce()
  // const debounceUpdate = useDebounce()
  const debounceSet = useDebounce(
    (value: T) => {
      store.set(value)
      downstreamChanged = false
      upstreamChanged = false
    },
    () => ((wait) => (wait === 'decoupled' ? 0 : wait))(wait())
  )
  const debounceUpdate = useDebounce(
    (updater: (value: T) => T) => {
      store.update(updater)
      downstreamChanged = false
      upstreamChanged = false
    },
    () => ((wait) => (wait === 'decoupled' ? 0 : wait))(wait())
  )
  return {
    subscribe(run: Subscriber<T>, invalidate?: ((value?: T) => void) | undefined) {
      const unsubState = state.subscribe(run, invalidate)
      const unsubChangeMonitor = store.subscribe(() => {
        upstreamChanged = true
      })
      return () => {
        unsubState()
        unsubChangeMonitor()
      }
    },
    async set(value: T) {
      state.set(value)
      downstreamChanged = true
      const wait = get(shouldWait).wait
      if (wait() === 'decoupled') {
        return
      }
      debounceSet(value)
    },
    async update(updater: (value: T) => T) {
      state.update(updater)
      downstreamChanged = true
      if (wait() === 'decoupled') {
        return
      }
      debounceUpdate(updater)
    },
    async pull() {
      const v = await store.load()
      state.set(v)
      upstreamChanged = false
      downstreamChanged = false
    },
    push() {
      store.set(get(state))
      upstreamChanged = false
      downstreamChanged = false
    },
    get upstreamChanged() {
      return upstreamChanged
    },
    get downstreamChanged() {
      return downstreamChanged
    },
    decouple() {
      shouldWait.update((old) => ({ wait: () => 'decoupled', last: old.wait }))
    },
    debounce(wait: number) {
      shouldWait.update((old) => ({ wait: () => wait, last: old.wait }))
    },
    toggle() {
      shouldWait.update((old) => ({ wait: old.last, last: old.wait }))
    }
  }
}
