/**
 * Upstream and downstream changes do not immediately affect each other
 * On `.pull()` upstream is applied to downstream
 * On `.push()` downstream is applied to upstream
 */
export const useDecoupledState = <T extends string | number | boolean>(
  upstream: { current: T },
  wait: () => number | 'decoupled'
) => {
  let upstreamChanged = $state(false)
  let downstreamChanged = $state(false)
  let downstream = $state({ current: upstream.current })

  let timeout = $state<NodeJS.Timeout>()

  const cancelDebounce = () => {
    clearTimeout(timeout)
    timeout = undefined
  }
  const debounceSet = () => {
    const periodMs = wait()
    if (periodMs === 'decoupled') {
      return
    }
    clearTimeout(timeout)
    timeout = setTimeout(() => {
      push()
    }, periodMs)
  }

  const push = () => {
    if (!downstreamChanged) {
      return
    }
    cancelDebounce()
    upstream.current = downstream.current
    upstreamChanged = false
    downstreamChanged = false
  }

  $effect(() => {
    upstream.current
    setTimeout(() => {
      const eq = upstream.current === downstream.current
      if (eq && upstreamChanged) {
        upstreamChanged = false
        return
      }
      if (eq) {
        return
      }
      upstreamChanged = true
    })
  })
  return {
    get current() {
      return downstream.current
    },
    set current(value: T) {
      downstream.current = value
      const isComparable =
        typeof value === 'string' ||
        typeof value === 'boolean' ||
        typeof value === 'number' ||
        typeof value === 'bigint'
      downstreamChanged = isComparable ? downstream.current !== upstream.current : true
      debounceSet()
    },
    pull() {
      clearTimeout(timeout)
      timeout = undefined
      downstream.current = upstream.current
      upstreamChanged = false
      downstreamChanged = false
    },
    push,
    cancelDebounce,
    get upstreamChanged() {
      return upstreamChanged
    },
    get downstreamChanged() {
      return downstreamChanged
    }
  }
}
