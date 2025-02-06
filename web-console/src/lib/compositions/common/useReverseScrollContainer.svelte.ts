import type { Action } from 'svelte/action'

export const useReverseScrollContainer = () => {
  let ref = <HTMLDivElement>undefined!
  let stickToBottom = $state(true)

  const scrollToBottom = (lastOffset?: number) => {
    if (!ref) {
      return
    }
    ref.scrollTo({
      top:
        ref.scrollHeight -
        (Math.round(ref.scrollHeight - ref.clientHeight) <= 0 ? ref.clientHeight : 0)
    })
  }

  let lastHeight: number

  const onResize = (change: 'expanded' | 'contracted') => {
    if (stickToBottom) {
      requestAnimationFrame(scrollToBottom)
    }
  }

  const resizeObserver = new ResizeObserver((entries) => {
    const entry = entries[0]
    if (!entry) {
      return
    }
    const newHeight = entry.contentRect.height
    if (newHeight !== lastHeight) {
      onResize(newHeight > lastHeight ? 'expanded' : 'contracted')
      lastHeight = newHeight
    }
  })

  const onscroll = () => {
    if (!ref || !(ref instanceof HTMLDivElement)) {
      return
    }
    stickToBottom = Math.round(ref.scrollTop - ref.scrollHeight + ref.clientHeight) >= 0
  }

  return {
    action: ((_node, props?: { observe?: (node: HTMLElement) => HTMLElement }) => {
      const node = props?.observe?.(_node) ?? _node
      lastHeight = node.offsetHeight
      if (!(node instanceof HTMLDivElement)) {
        return
      }
      ref = node
      resizeObserver.observe(node.firstElementChild!)
      node.addEventListener('scroll', onscroll)
      scrollToBottom()
      return {
        destroy: () => {
          resizeObserver.disconnect()
          node.removeEventListener('scroll', onscroll)
        }
      }
    }) satisfies Action,
    scrollToBottom,
    get stickToBottom() {
      return stickToBottom
    },
    set stickToBottom(value: boolean) {
      stickToBottom = value
    }
  }
}
