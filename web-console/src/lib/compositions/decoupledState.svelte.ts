const isEqual = <T>(a: T, b: T) => {
  const isComparable =
    typeof a === 'string' ||
    typeof a === 'boolean' ||
    typeof a === 'number' ||
    typeof a === 'bigint'
  return isComparable ? a === b : false
}

/**
 * Upstream and downstream changes do not immediately affect each other.
 *
 * Downstream changes are debounced for `wait` milliseconds before setting `upstream` unless `wait` is 'decoupled'.
 *
 * On `.pull()` upstream is applied to downstream.
 * On `.push()` downstream is applied to upstream.
 */
export class DecoupledStateProxy<T extends string | number | boolean> {
  protected downstream: { current: T } = $state({ current: undefined! })
  protected baseline: T
  protected _upstream: { current: T } = { current: undefined! }
  protected _upstreamChanged = $state(false)
  protected _downstreamChanged = $state(false)
  protected wait: () => number | 'decoupled'
  protected debounceSet: () => void
  // @ts-ignore:next-line
  protected timeout: NodeJS.Timeout

  constructor(
    upstream: { current: T },
    downstream: { current: T },
    wait: () => number | 'decoupled'
  ) {
    this.downstream = downstream
    // this.downstream.current = upstream.current
    this.baseline = upstream.current
    this._upstream = upstream
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
  }

  touch() {
    this._downstreamChanged = !isEqual(this.baseline, this.downstream.current)
    this.debounceSet()
  }

  fetch(newUpstream?: { current: T }) {
    if (newUpstream) {
      this._upstream = newUpstream
    }
    const eq = isEqual(this._upstream.current, this.baseline)
    if (eq && this._upstreamChanged) {
      this._upstreamChanged = false
      return
    }
    if (eq) {
      return
    }
    if (isEqual(this._upstream.current, this.downstream.current)) {
      // Avoid prompting conflict resolution if local changes match new upstream value
      this.baseline = this._upstream.current
      this._downstreamChanged = false
      return
    }
    this._upstreamChanged = true
  }

  pull() {
    this.cancelDebounce()
    this.baseline = this.downstream.current = this._upstream.current
    this._upstreamChanged = false
    this._downstreamChanged = false
  }
  push() {
    if (!this._downstreamChanged && !this.upstreamChanged) {
      return
    }
    this.baseline = this._upstream.current = this.downstream.current
    this._upstreamChanged = false
    this._downstreamChanged = false
  }
  get upstreamChanged() {
    return this._upstreamChanged
  }
  get downstreamChanged() {
    return this._downstreamChanged
  }
  cancelDebounce = () => {
    clearTimeout(this.timeout)
    this.timeout = undefined!
  };
  [Symbol.dispose]() {
    this.cancelDebounce()
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
export class DecoupledState<T extends string | number | boolean> {
  protected downstream: { current: T } = $state({ current: undefined! })
  protected _upstream: { current: T } = $state({ current: undefined! })
  protected _upstreamChanged = $state(false)
  protected _downstreamChanged = $state(false)
  protected wait: () => number | 'decoupled'
  protected debounceSet: () => void
  // @ts-ignore:next-line
  protected timeout: NodeJS.Timeout

  handleUpstreamUpdate() {
    const eq = isEqual(this._upstream.current, this.downstream.current)
    if (eq && this._upstreamChanged) {
      this._upstreamChanged = false
      return
    }
    if (eq) {
      return
    }
    if (!eq && this._downstreamChanged) {
      return
    }
    this._upstreamChanged = true
  }

  constructor(upstream: { current: T }, wait: () => number | 'decoupled') {
    this.downstream.current = upstream.current
    this._upstream = upstream
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
  }

  get current() {
    return this.downstream.current
  }
  set current(value: T) {
    this.downstream.current = value
    this._downstreamChanged = !isEqual(this._upstream.current, value)
    this.debounceSet()
  }
  fetch() {
    this.handleUpstreamUpdate()
  }

  pull() {
    clearTimeout(this.timeout)
    this.timeout = undefined!
    this.downstream.current = this._upstream.current
    this._upstreamChanged = false
    this._downstreamChanged = false
  }
  push() {
    if (!this._downstreamChanged) {
      return
    }
    this._upstream.current = this.downstream.current
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
      const eq = isEqual(upstream.current, downstream.current)
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
      downstreamChanged = !isEqual(upstream.current, value)
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
