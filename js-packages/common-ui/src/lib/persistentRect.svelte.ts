import type { Action } from 'svelte/action'

/**
 * Shared rect state for the persistent-overlay pair. The placeholder writes its bounding
 * rect into this object as it mounts, resizes, scrolls, etc.; <PersistentContent> reads
 * from it and mirrors the rect onto a fixed-position overlay so the content (which is
 * rendered once outside the placeholder's conditional) visually tracks the placeholder
 * across remounts.
 */
export type PersistentRect = {
  left: number
  top: number
  width: number
  height: number
  visible: boolean
}

/**
 * Handle returned by `usePersistentRect`. Pair the two ends:
 * - `placeholder` — Svelte action applied to a regular `<div use:handle.placeholder>`.
 *   Tracks the element's rect via ResizeObserver + window resize/scroll listeners and
 *   writes it into `rect`.
 * - Pass the whole handle to <PersistentContent persistent={handle}>, which reads `rect`
 *   and renders its children into a fixed-position wrapper sized to match.
 */
export type PersistentHandle = {
  rect: PersistentRect
  placeholder: Action<HTMLElement>
}

export function usePersistentRect(): PersistentHandle {
  let x = $state({ left: 0, top: 0, width: 0, height: 0, visible: false })
  const placeholder: Action<HTMLElement> = (node) => {
    const update = () => {
      const r = node.getBoundingClientRect()
      x.left = r.left
      x.top = r.top
      x.width = r.width
      x.height = r.height
      x.visible = r.width > 0 && r.height > 0
    }
    update()
    const ro = new ResizeObserver(update)
    ro.observe(node)
    window.addEventListener('resize', update)
    // Capture-phase scroll: catches any scrolling ancestor, not just window.
    window.addEventListener('scroll', update, true)
    return {
      destroy() {
        ro.disconnect()
        window.removeEventListener('resize', update)
        window.removeEventListener('scroll', update, true)
        x.visible = false
      }
    }
  }
  return {
    get rect() {
      return x
    },
    placeholder
  }
}
