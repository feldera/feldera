import { type AfterScrollEvent } from 'svelte-virtuallists'
import { useResizeObserver } from 'runed'

export const useElementSize = (getRef: () => HTMLElement | undefined) => {
  let height = $state(0)
  let width = $state(0)
  useResizeObserver(getRef, (entries) => {
    const entry = entries[0]
    if (!entry) {
      return
    }
    height = entry.contentRect.height
    width = entry.contentRect.width
  })
  return {
    get height() {
      return height
    },
    get width() {
      return width
    }
  }
}

export const useStickScrollToBottom = (
  height: () => number,
  data: () => any[],
  itemSize: number | number[]
) => {
  let len = $derived(data().length)
  let lastLen = $state(data().length)
  let scrollOffset = $state(0)
  let lastScrollOffset = $state(0)
  let didFirstScroll = $state(false)
  const onAfterScroll = (e: AfterScrollEvent) => {
    lastScrollOffset = Number(e.offset)
  }
  $effect(() => {
    if (height() === 0) {
      return
    }
    if (lastLen === len && didFirstScroll) {
      return
    }
    stickToBottom(lastLen, len)
    lastLen = len
  })
  const scrollHeightAtLength = (len: number) =>
    Array.isArray(itemSize)
      ? itemSize.reduce((acc, cur, i) => (i < len ? acc + cur : acc), 0)
      : len * itemSize
  const stickToBottom = (lastLen: number, len: number) => {
    if (
      lastScrollOffset !== 0 &&
      Math.round(lastScrollOffset + height()) >= scrollHeightAtLength(lastLen)
    ) {
      // Scroll to the new bottom of the list if scroll was at the bottom previously
      lastScrollOffset = scrollOffset = scrollHeightAtLength(len)
    } else if (!didFirstScroll && scrollHeightAtLength(len) > height()) {
      // Scroll to the bottom of the list the first time it became longer than the viewport
      lastScrollOffset = scrollOffset = scrollHeightAtLength(len)
      didFirstScroll = true
    }
  }
  return {
    get scrollOffset() {
      return scrollOffset
    },
    onAfterScroll,
    get isAtBottom() {
      return height() === 0 || Math.round(lastScrollOffset + height()) >= scrollHeightAtLength(len)
    },
    scrolToBottom() {
      // Force scroll to bottom
      scrollOffset = undefined!
      setTimeout(() => (scrollOffset = scrollHeightAtLength(len)))
    }
  }
}
