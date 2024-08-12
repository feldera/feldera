import { useDebounce } from 'runed'

/**
 * Upstream and downstream changes do not immediately affect each other
 * On `.pull()` upstream is applied to downstream
 * On `.push()` downstream is applied to upstream
 */
export const useDecoupledState = <T>(
  upstream: { current: T },
  wait: () => number | 'decoupled'
) => {
  let upstreamChanged = $state(false)
  let downstreamChanged = $state(false)
  let downstream = $state({ current: upstream.current })

  const debounceSet = useDebounce(
    (value: T) => {
      upstream.current = value
      downstreamChanged = false
      upstreamChanged = false
    },
    () => ((wait) => (wait === 'decoupled' ? 0 : wait))(wait())
  )
  $effect(() => {
    void upstream.current
    upstreamChanged = true
  })
  return {
    get current() {
      return downstream.current
    },
    set current(value: T) {
      downstream.current = value
      downstreamChanged = true
      if (wait() === 'decoupled') {
        return
      }
      debounceSet(value)
    },
    pull() {
      downstream.current = upstream.current
      upstreamChanged = false
      downstreamChanged = false
    },
    push() {
      if (!downstreamChanged) {
        return
      }
      upstream.current = downstream.current
      upstreamChanged = false
      downstreamChanged = false
    },
    get upstreamChanged() {
      return upstreamChanged
    },
    get downstreamChanged() {
      return downstreamChanged
    }
  }
}
