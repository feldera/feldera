import type { Action } from 'svelte/action'

/**
 * Writes the width of the node's reserved scrollbar gutter to `--scroll-gutter-width` on
 * `<body>`, so page gutters can make up for it. Overlay scrollbars (macOS by default)
 * reserve no gutter, so measure the real one. It is not `--scrollbar-width`, which sets the
 * scrollbar's width: a measurement taken before that applies would then hold it at the old
 * width.
 */
export const measureScrollGutter: Action<HTMLElement> = (node) => {
  const measure = () =>
    document.body.style.setProperty(
      '--scroll-gutter-width',
      `${node.offsetWidth - node.clientWidth}px`
    )
  measure()
  const observer = new ResizeObserver(measure)
  observer.observe(node)
  return {
    destroy() {
      observer.disconnect()
      document.body.style.removeProperty('--scroll-gutter-width')
    }
  }
}
