import { Debounced } from 'runed'
import type { Action } from 'svelte/action'

export const useReverseScrollContainer = (
  params: (
    | {
        observeContentElement: (node: HTMLElement) => Element
      }
    | {
        observeContentSize: () => number
      }
  ) & {
    /** Initial value of `stickToBottom`. Defaults to `true` (streaming-log behaviour: start at
     *  the bottom, re-stick as content grows). Pass `false` for static content that should
     *  start at the top and never auto-scroll on mount. */
    initialStickToBottom?: boolean
    /** Fired on a stick-to-bottom transition: `true` when the view anchors to the bottom,
     *  `false` when the user scrolls up off it. */
    onStickToBottomChange?: (stickToBottom: boolean) => void
  }
) => {
  let ref = <HTMLDivElement>undefined!
  let stickToBottom = $state(params.initialStickToBottom ?? true)

  const scrollToBottomImpl = () => {
    if (!ref) {
      return
    }

    ref.scrollTo({
      top:
        ref.scrollHeight -
        (Math.round(ref.scrollHeight - ref.clientHeight) >= 0 ? ref.clientHeight : 0)
    })
  }

  // Scrolling to the bottom is not finished in one go: it is executed in two animation frames
  // (see `scrollToBottom` below). This counter is how a scheduled scroll learns that it is no
  // longer wanted. It goes up whenever a caller sets `stickToBottom` to false, which means the
  // scroll was set programmatically. A waiting scroll remembers the count it was scheduled with,
  // and does nothing if that count has changed by the time it runs.
  let unstickCount = 0

  // Run `scroll` on the next animation frame, unless the caller moves the view away in between.
  const queueScrollPass = (scroll: () => void) => {
    const unstickCountWhenQueued = unstickCount
    requestAnimationFrame(() => {
      if (unstickCountWhenQueued === unstickCount) {
        scroll()
      }
    })
  }

  // Scroll down twice, one frame apart. Rows are measured only once they are on the page, so the
  // first scroll usually brings more of them into view and pushes the real bottom further down.
  const scrollToBottom = () => {
    scrollToBottomImpl()
    queueScrollPass(scrollToBottomImpl)
  }

  let lastSize: number = 0

  const onResize = (change: 'expanded' | 'contracted') => {
    if (stickToBottom) {
      queueScrollPass(scrollToBottom)
    }
  }

  const onscroll = () => {
    if (!ref || !(ref instanceof HTMLDivElement)) {
      return
    }
    stickToBottom = Math.round(ref.scrollTop - ref.scrollHeight + ref.clientHeight + 1) >= 0 // + 1 pixel is added to account for imprecision between calculated .scrollTop and .scrollHeight
  }

  const debouncedStickToBottom = new Debounced(() => stickToBottom, 50)

  // Notify the parent on stick-to-bottom transitions.
  // The effect re-runs only when the debounced value actually changes.
  let lastNotifiedStick = debouncedStickToBottom.current
  $effect(() => {
    const stick = debouncedStickToBottom.current
    if (stick !== lastNotifiedStick) {
      lastNotifiedStick = stick
      params.onStickToBottomChange?.(stick)
    }
  })

  return {
    action: ((
      _node,
      props?: {
        getContainerElement?: (node: HTMLElement) => HTMLElement
      }
    ) => {
      const node = props?.getContainerElement?.(_node) ?? _node
      lastSize = node.offsetHeight
      if (!(node instanceof HTMLDivElement)) {
        return
      }
      ref = node

      const resizeObserver =
        'observeContentElement' in params
          ? (() => {
              const observer = new ResizeObserver((entries) => {
                const entry = entries[0]
                if (!entry) {
                  return
                }
                const newHeight = entry.contentRect.height
                if (newHeight !== lastSize) {
                  onResize(newHeight > lastSize ? 'expanded' : 'contracted')
                  lastSize = newHeight
                }
              })
              observer.observe(params.observeContentElement(ref))
              return observer
            })()
          : undefined

      if ('observeContentSize' in params) {
        $effect(() => {
          const newSize = params.observeContentSize()
          if (newSize !== lastSize) {
            onResize(newSize > lastSize ? 'expanded' : 'contracted')
            lastSize = newSize
            onscroll()
          }
        })
      }

      // When the container becomes visible after being hidden (e.g. tab switch with keepAlive),
      // the ResizeObserver fires but the virtualizer may need extra time to lay out content.
      // Schedule scrollToBottom attempt to catch up.
      let wasHidden = false
      const visibilityObserver = new IntersectionObserver((entries) => {
        const isVisible = entries[0]?.isIntersecting ?? false
        if (isVisible && wasHidden && stickToBottom) {
          scrollToBottom()
        }
        wasHidden = !isVisible
      })
      visibilityObserver.observe(ref)

      ref.addEventListener('scroll', onscroll)
      // Honour `initialStickToBottom` — static-content consumers (bundle log dumps, etc.)
      // want the view to mount at the top, not yank to the bottom on first paint.
      if (stickToBottom) {
        scrollToBottom()
      }
      return {
        destroy: () => {
          resizeObserver?.disconnect()
          visibilityObserver.disconnect()
          ref.removeEventListener('scroll', onscroll)
        }
      }
    }) satisfies Action<HTMLDivElement>,
    scrollToBottom,
    get stickToBottom() {
      return debouncedStickToBottom.current
    },
    set stickToBottom(value: boolean) {
      if (!value) {
        // The caller is taking the view somewhere else, so drop any scroll to the bottom that is
        // still waiting for its animation frame. See `unstickCount` above.
        unstickCount++
      }
      stickToBottom = value
    }
  }
}
