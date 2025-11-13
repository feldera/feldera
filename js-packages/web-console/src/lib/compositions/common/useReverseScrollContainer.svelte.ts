import { Debounced } from 'runed'
import type { Action } from 'svelte/action'

export const useReverseScrollContainer = (
  params:
    | {
        observeContentElement: (node: HTMLElement) => Element
      }
    | {
        observeContentSize: () => number
      }
) => {
  let ref = <HTMLDivElement>undefined!
  let stickToBottom = $state(true)

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

  const scrollToBottom = () => {
    scrollToBottomImpl()
    requestAnimationFrame(scrollToBottomImpl)
  }

  let lastSize: number = 0

  const onResize = (change: 'expanded' | 'contracted') => {
    if (stickToBottom) {
      requestAnimationFrame(scrollToBottom)
    }
  }

  const onscroll = () => {
    if (!ref || !(ref instanceof HTMLDivElement)) {
      return
    }
    stickToBottom = Math.round(ref.scrollTop - ref.scrollHeight + ref.clientHeight + 1) >= 0 // + 1 pixel is added to account for imprecision between calculated .scrollTop and .scrollHeight
  }

  const debouncedStickToBottom = new Debounced(() => stickToBottom, 50)

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

      ref.addEventListener('scroll', onscroll)
      scrollToBottom()
      return {
        destroy: () => {
          resizeObserver?.disconnect()
          ref.removeEventListener('scroll', onscroll)
        }
      }
    }) satisfies Action<HTMLDivElement>,
    scrollToBottom,
    get stickToBottom() {
      return debouncedStickToBottom.current
    },
    set stickToBottom(value: boolean) {
      stickToBottom = value
    }
  }
}
