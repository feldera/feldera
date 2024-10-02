/**
 * Upstream and downstream changes do not immediately affect each other.
 *
 * Downstream changes are debounced for `wait` milliseconds before setting `upstream` unless `wait` is 'decoupled'.
 *
 * On `.pull()` upstream is applied to downstream.
 * On `.push()` downstream is applied to upstream.
 */
export class DecoupledState<T extends string | number | boolean> {
  downstream: { current: T } = $state({ current: undefined! })
  upstream: { current: T }
  _upstreamChanged = $state(false)
  _downstreamChanged = $state(false)
  wait: () => number | 'decoupled'
  debounceSet: () => void
  // @ts-ignore:next-line
  timeout: NodeJS.Timeout

  constructor(upstream: { current: T }, wait: () => number | 'decoupled') {
    this.downstream.current = upstream.current
    this.upstream = upstream
    this.wait = wait
    this.debounceSet = () => {
      const periodMs = wait()
      if (periodMs === 'decoupled') {
        return
      }
      clearTimeout(this.timeout)
      this.timeout = setTimeout(() => {
        if (wait() === 'decoupled') {
          return
        }
        this.push()
      }, periodMs)
    }

    $effect(() => {
      upstream.current
      setTimeout(() => {
        const eq = this.upstream.current === this.downstream.current
        if (eq && this._upstreamChanged) {
          this._upstreamChanged = false
          return
        }
        if (eq) {
          return
        }
        this._upstreamChanged = true
      })
    })
  }

  get current() {
    return this.downstream.current
  }
  set current(value: T) {
    this.downstream.current = value

    const isComparable =
      typeof value === 'string' ||
      typeof value === 'boolean' ||
      typeof value === 'number' ||
      typeof value === 'bigint'
    this._downstreamChanged = isComparable ? this.upstream.current !== value : true
    this.debounceSet()
  }

  pull() {
    clearTimeout(this.timeout)
    this.timeout = undefined!
    this.downstream.current = this.upstream.current
    this._upstreamChanged = false
    this._downstreamChanged = false
  }
  push() {
    if (!this._downstreamChanged) {
      return
    }
    this.upstream.current = this.downstream.current
    this._upstreamChanged = false
    this._downstreamChanged = false
  }
  get upstreamChanged() {
    return this._upstreamChanged
  }
  get downstreamChanged() {
    return this._downstreamChanged
  }
}

/**
 * Upstream and downstream changes do not immediately affect each other.
 *
 * Downstream changes are debounced for `wait` milliseconds before setting `upstream` unless `wait` is 'decoupled'.
 *
 * On `.pull()` upstream is applied to downstream.
 * On `.push()` downstream is applied to upstream.
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
      if (wait() === 'decoupled') {
        return
      }
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
      downstreamChanged = isComparable ? upstream.current !== value : true
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
