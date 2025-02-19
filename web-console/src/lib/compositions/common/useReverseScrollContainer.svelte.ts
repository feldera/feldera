import { Debounced } from 'runed'
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
        (Math.round(ref.scrollHeight - ref.clientHeight) >= 0 ? ref.clientHeight : 0)
    })
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
    action: ((_node, props) => {
      const node = props?.getContainerElement?.(_node) ?? _node
      lastSize = node.offsetHeight
      if (!(node instanceof HTMLDivElement)) {
        return
      }
      ref = node

      const resizeObserver =
        'observeContentElement' in props
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
              observer.observe(props.observeContentElement(ref))
              return observer
            })()
          : undefined

      if ('observeContentSize' in props) {
        $effect(() => {
          const newSize = props.observeContentSize()
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
    }) satisfies Action<
      HTMLDivElement,
      {
        getContainerElement?: (node: HTMLElement) => HTMLElement
      } & (
        | {
            observeContentElement: (node: HTMLElement) => Element
          }
        | {
            observeContentSize: () => number
          }
      )
    >,
    scrollToBottom,
    get stickToBottom() {
      return debouncedStickToBottom.current
    },
    set stickToBottom(value: boolean) {
      stickToBottom = value
    }
  }
}
