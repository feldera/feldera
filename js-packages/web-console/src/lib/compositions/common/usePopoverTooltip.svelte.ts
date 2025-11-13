import { untrack } from 'svelte'

export const usePopoverTooltip = (popoverElement: () => HTMLElement | undefined) => {
  let tooltipDetails: { element: HTMLElement; value: any } | undefined = $state()
  let popupData: { x: number; y: number; targetWidth: number; text: string } | undefined = $state()
  let hoverCount = $state(0)
  $effect(() => {
    if (hoverCount > 3) {
      hoverCount = 3
    }
  })
  $effect(() => {
    const el = popoverElement()
    if (!el) {
      return
    }
    const onmouseenter = untrack(() => () => {
      ++hoverCount
    })
    const onmouseleave = untrack(() => () => {
      requestAnimationFrame(() => {
        hoverCount -= hoverCount > 0 ? 1 : 0
      })
    })
    const onwheel = () => {
      // Allow scrolling underlying container although tooltip may intercept wheel event
      const el = popoverElement()
      if (!el) {
        return
      }
      el.style.pointerEvents = 'none'
      setTimeout(() => {
        el.style.pointerEvents = 'all'
      }, 100)
    }
    el.addEventListener('mouseenter', onmouseenter)
    el.addEventListener('mouseleave', onmouseleave)
    // el.addEventListener('wheel', onwheel)
    return () => {
      el.removeEventListener('mouseenter', onmouseenter)
      el.removeEventListener('mouseleave', onmouseleave)
      // el.removeEventListener('wheel', onwheel)
      hoverCount = 0
    }
  })
  let showTooltip = $derived(hoverCount > 0)
  $effect(() => {
    const el = popoverElement()
    if (!el) {
      return
    }
    if (!showTooltip) {
      tooltipDetails = undefined
    }
    if (!tooltipDetails) {
      popupData = undefined
      el.hidePopover()
      return
    }
    const text = tooltipDetails.value
    const rect = tooltipDetails.element.getBoundingClientRect()
    popupData = {
      x: 0,
      y: 0,
      targetWidth: 0,
      text
    }
    el.showPopover()
    requestAnimationFrame(() => {
      const rect2 = el.getBoundingClientRect()
      popupData = {
        x: Math.min(Math.max(0, rect.left), window.innerWidth - rect2.width),
        y: Math.min(rect.top, window.innerHeight - rect2.height),
        targetWidth: tooltipDetails!.element.clientWidth,
        text
      }
    })
  })
  type CustomMouseEvent = MouseEvent & {
    currentTarget: EventTarget & HTMLTableCellElement
  }
  const showTooltipWith = (e: CustomMouseEvent, value: any) => {
    tooltipDetails = { element: e.currentTarget, value }
    ++hoverCount
  }
  const showTooltipCb = (value: any) => (e: CustomMouseEvent) => showTooltipWith(e, value)
  const onmouseleave = (e: CustomMouseEvent) => {
    requestAnimationFrame(() => {
      hoverCount -= hoverCount > 0 ? 1 : 0
    })
  }
  return {
    get data() {
      return popupData
    },
    showTooltip: showTooltipCb,
    onmouseleave,
    onclick
  }
}
