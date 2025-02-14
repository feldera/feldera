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

  const mutationObserver = new MutationObserver(() => {
    if (stickToBottom) {
      requestAnimationFrame(scrollToBottom)
    }
  })

  const onscroll = () => {
    if (!ref || !(ref instanceof HTMLDivElement)) {
      return
    }
    stickToBottom = Math.round(ref.scrollTop - ref.scrollHeight + ref.clientHeight) >= 0
  }

  return {
    action: ((node) => {
      if (!(node instanceof HTMLDivElement)) {
        return
      }
      ref = node
      mutationObserver.observe(node, { childList: true })
      node.addEventListener('scroll', onscroll)
      scrollToBottom()
      return {
        // update: (updatedParameter) => {...},
        destroy: () => {
          mutationObserver.disconnect()
          node.removeEventListener('scroll', onscroll)
        }
      }
    }) as Action,
    scrollToBottom,
    get stickToBottom() {
      return stickToBottom
    },
    set stickToBottom(value: boolean) {
      stickToBottom = value
    }
  }
}
