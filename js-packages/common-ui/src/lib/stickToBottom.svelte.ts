/**
 * The one scroll anchor. Every view in the product that follows growing content uses it: the log
 * viewer, the change stream, and the ad-hoc query results.
 *
 * It is deliberately indifferent to how the content is rendered. Anchoring is expressed as a
 * fixed-point search on the container's own geometry, which is the one thing a plain div, a
 * fixed-height virtual list and a measuring virtualiser all report the same way.
 *
 * Two collaborators keep the parts that are not about geometry out of here: `releaseGestures`
 * decides what counts as the user taking over, and `viewportDormancy` decides when the geometry is
 * worth reading at all.
 */

import { untrack } from 'svelte'
import type { Action } from 'svelte/action'
import { observeReleaseGestures } from './releaseGestures'
import { observeViewportDormancy } from './viewportDormancy'

/**
 * Bottom tolerance in pixels.
 *
 * `scrollHeight`, `scrollTop` and `clientHeight` round independently, and on a fractional device
 * pixel ratio (125% or 150% display scaling) the residual at a visually pinned bottom settles
 * between 1 and 2px. A 1px window rejects that state forever, leaving the FAB on screen with
 * nowhere left to scroll.
 */
const STICK_EPS = 3

/**
 * Upper bound on settle passes.
 *
 * Each pass reveals tail content the virtualiser had only estimated, which grows `scrollHeight`
 * and moves the bottom further down. Measured across the log-view suites, real content converges
 * within one pass, and a fixture built to under-estimate its height fifteenfold peaks at eight, so
 * this is a backstop against a container whose height never stabilises rather than a working
 * limit. Reaching it strands nothing: the next append requests another settle.
 */
const MAX_SETTLE_PASSES = 32

/** How long `stuck` must hold a value before {@link StickToBottomOptions.onChange} reports it. */
const CHANGE_DEBOUNCE_MS = 50

export type StickToBottomOptions = {
  /** Initial state. Streaming feeds start stuck; static dumps at the top. Defaults to true. */
  initial?: boolean
  /**
   * Re-anchor whenever this number changes. For a fixed-height virtual list, the row count.
   * Read reactively, so the caller must invoke `useStickToBottom` during component init.
   */
  observeSize?: () => number
  /**
   * Re-anchor whenever this element's height changes. For containers sized by their content, where
   * there is no count to watch.
   *
   * Read reactively, like {@link StickToBottomOptions.observeSize}: return a `bind:this` target and
   * the observer follows it, so content that is not in the DOM at mount is picked up when it
   * arrives and a replaced element is re-observed rather than silently abandoned.
   */
  observeElement?: () => Element | null | undefined
  /**
   * Fired on a debounced transition of {@link StickToBottom.stuck}. Hosts pause and resume feeds
   * on it, so it deliberately lags the raw value rather than reporting every flicker.
   */
  onChange?: (stuck: boolean) => void
}

export type StickToBottom = {
  /** Applied to the scroll container. */
  action: Action<HTMLElement>
  /**
   * Raw and undebounced, so a FAB driven by it hides on the press rather than a frame later.
   * Read-only: use {@link StickToBottom.stick} and {@link StickToBottom.release} to change it.
   */
  readonly stuck: boolean
  /** Re-arm and settle to the bottom. Idempotent; safe to call while a settle is in flight. */
  stick(): void
  /** Give up the bottom. Call before any programmatic jump somewhere else. */
  release(): void
  /**
   * Settle again if, and only if, the view is still stuck.
   *
   * For a caller that rebuilds the thing being scrolled — a virtualiser remounted around a fresh
   * size cache — where the scroll position is lost but the user's intent to follow the end is not.
   * Unlike {@link StickToBottom.stick} it never re-arms a view the user has scrolled away from.
   */
  refresh(): void
}

/**
 * Anchors a scroll container to its bottom as content grows.
 *
 * Two rules make this reliable where a fixed number of scroll passes does not:
 *
 * 1. Settling iterates to a fixed point. The target is `scrollHeight`, which is only exact once
 *    every element between here and the end has been measured. Scrolling there mounts more of
 *    them, which moves the target. Stopping after a set number of passes leaves the view short by
 *    however much the estimate was wrong, which is what makes a scroll-to-bottom button need
 *    pressing twice.
 *
 * 2. Releasing is driven by input events, never by geometry. A scroll event cannot say who caused
 *    it, and virtualisers write `scrollTop` themselves when a freshly measured row changes the
 *    layout. Treating those writes as user intent cancels the settle midway through.
 */
export const useStickToBottom = (options: StickToBottomOptions = {}): StickToBottom => {
  let container: HTMLElement | undefined
  let stuck = $state(options.initial ?? true)
  let dormancy: { readonly dormant: boolean; wake(): void } | undefined

  /**
   * The settle in flight, or undefined when nothing is driving the scroll.
   *
   * One value does three jobs. It marks which scroll events are ours, so geometry cannot unstick a
   * view we are moving ourselves. It coalesces the triggers below, since a second request while
   * one is in flight is redundant: every pass re-reads the geometry and aims again, so the running
   * loop already accounts for whatever the new trigger noticed. And it gives a caller something to
   * wait on.
   */
  let settling: Promise<void> | undefined

  /** Bumped whenever the view is deliberately sent elsewhere, to strand an in-flight settle. */
  let generation = 0

  const distanceFromBottom = () =>
    container ? container.scrollHeight - container.scrollTop - container.clientHeight : 0

  const atBottom = () => distanceFromBottom() <= STICK_EPS

  /**
   * Write `scrollTop` and iterate until the scroll height stops moving.
   *
   * `defer` waits a frame before the first write, which a trigger firing inside a ResizeObserver
   * callback has to do: writing `scrollTop` from there mounts virtualised rows, which resizes
   * them, which queues another notification in the same delivery cycle — the "undelivered
   * notifications" loop error. The deferred frame still counts as ours, because `settling` is
   * assigned before it.
   */
  const settleToBottom = (defer: boolean): Promise<void> => {
    if (!container) {
      return Promise.resolve()
    }
    const settleGeneration = generation
    let passes = 0
    let finish: () => void
    const settled = new Promise<void>((resolve) => {
      finish = resolve
    })
    settling = settled

    const done = () => {
      // Only when the slot is still ours. `release` strands this loop, and a later one may already
      // own the slot by the time the stranded frame fires.
      if (settling === settled) {
        settling = undefined
      }
      finish()
    }

    /** The container while this settle still speaks for the view, and undefined once it does not. */
    const claimed = () => (settleGeneration === generation && stuck ? container : undefined)

    const step = () => {
      const el = claimed()
      if (!el) {
        done()
        return
      }
      // Assigning the full scroll height lets the browser clamp to the true maximum. Subtracting
      // `clientHeight` ourselves reintroduces the rounding error the tolerance exists to absorb.
      const heightBefore = el.scrollHeight
      el.scrollTop = heightBefore

      requestAnimationFrame(() => {
        const current = claimed()
        if (!current) {
          done()
          return
        }
        const converged = current.scrollHeight === heightBefore && atBottom()
        if (converged || ++passes >= MAX_SETTLE_PASSES) {
          done()
          return
        }
        step()
      })
    }

    if (defer) {
      requestAnimationFrame(step)
    } else {
      step()
    }
    return settled
  }

  // `onChange` reports intent, not every intermediate reading, so it waits for the value to hold.
  let changeTimer: ReturnType<typeof setTimeout> | undefined
  let lastReported = options.initial ?? true
  const setStuck = (value: boolean) => {
    if (stuck === value) {
      return
    }
    stuck = value
    const onChange = options.onChange
    if (!onChange) {
      return
    }
    clearTimeout(changeTimer)
    changeTimer = setTimeout(() => {
      if (stuck !== lastReported) {
        lastReported = stuck
        onChange(stuck)
      }
    }, CHANGE_DEBOUNCE_MS)
  }

  /**
   * Settle to the bottom, if the view is still stuck.
   *
   * Every trigger funnels through here, which is what keeps a single settle running. Five of them
   * can fire in one frame on a resize of a streaming log, and unfunnelled they did: the log-view
   * suites recorded up to five rAF chains writing `scrollTop` at once, whereupon the first to
   * finish declared the scroll no longer ours while the rest were still moving it.
   */
  const requestSettle = (defer = false): Promise<void> => {
    if (!stuck) {
      return Promise.resolve()
    }
    return settling ?? settleToBottom(defer)
  }

  const stick = () => {
    setStuck(true)
    void requestSettle()
  }

  const release = () => {
    generation++
    settling = undefined
    // A release only ever comes from a user gesture, which proves the container is on screen.
    dormancy?.wake()
    setStuck(false)
  }

  const refresh = () => {
    void requestSettle()
  }

  const onScroll = () => {
    // Our own writes, and those a virtualiser makes while correcting a measured row, both arrive
    // here indistinguishable from a user scroll. During a settle they are all ours, and while the
    // container is dormant none of them describe where the user wants to be.
    if (settling || dormancy?.dormant) {
      return
    }
    setStuck(atBottom())
  }

  const action: StickToBottom['action'] = (node) => {
    container = node

    node.addEventListener('scroll', onScroll, { passive: true })
    const detachGestures = observeReleaseGestures(node, release)

    // A container hidden behind an inactive tab reports a zero-height viewport, so anything that
    // arrived while it was away landed against the wrong geometry. Re-settle once it is back.
    const viewport = observeViewportDormancy(node, () => void requestSettle())
    dormancy = viewport

    const observeElement = options.observeElement
    if (observeElement) {
      $effect(() => {
        const target = observeElement()
        if (!target) {
          return
        }
        // Per target rather than shared: a replacement that happens to start at the height the
        // previous one ended at is still a new element, and its first report is not a no-op.
        let lastHeight = 0
        const observer = new ResizeObserver((entries) => {
          const height = entries[0]?.contentRect.height
          if (height === undefined || height === lastHeight) {
            return
          }
          lastHeight = height
          void requestSettle(true)
        })
        observer.observe(target)
        return () => observer.disconnect()
      })
    }

    const observeSize = options.observeSize
    if (observeSize) {
      let lastSize: number | undefined
      $effect(() => {
        const size = observeSize()
        if (size === lastSize) {
          return
        }
        lastSize = size
        // Settled at once rather than on the next frame. An effect is not a resize notification,
        // so nothing here can loop, and the frame a deferral costs is a frame in which a streaming
        // list is painted somewhere other than its end — which is the whole thing being avoided.
        untrack(() => {
          void requestSettle()
        })
      })
    }

    // The container's own size is as much a part of "how far from the bottom are we" as the
    // content's. Shrinking the viewport leaves the content where it is and moves the bottom away,
    // and the browser reports that as a scroll — which reads as the user scrolling up and unsticks
    // a view that never moved. Requesting the settle here claims the geometry from this callback
    // until the view arrives, so that scroll event is read as ours.
    const containerObserver = new ResizeObserver(() => {
      void requestSettle(true)
    })
    containerObserver.observe(node)

    void requestSettle()

    return {
      destroy: () => {
        // Any settle still in flight reads `container` on its next frame and stops there.
        container = undefined
        dormancy = undefined
        containerObserver.disconnect()
        viewport.disconnect()
        detachGestures()
        node.removeEventListener('scroll', onScroll)
        clearTimeout(changeTimer)
      }
    }
  }

  return {
    action,
    get stuck() {
      return stuck
    },
    stick,
    release,
    refresh
  }
}
