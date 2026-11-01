<script lang="ts">
  /**
   * A drag handle on the right edge of a table header cell that resizes its column. Place it
   * inside a `relative` header cell carrying the `group/col` class, which reveals the handle
   * on hover. Dragging, ←/→ and double-click (back to the default width) all report through
   * the callbacks; the width itself is owned by the table.
   */
  const {
    width,
    min,
    label,
    onResize,
    onReset
  }: {
    /** The column's current width, in px. */
    width: number
    /** The narrowest the column may become, in px. */
    min: number
    /** The column's name, for screen readers. */
    label: string
    onResize: (width: number) => void
    onReset: () => void
  } = $props()

  const KEYBOARD_STEP = 16

  let drag = $state<{ pointerId: number; startX: number; startWidth: number } | null>(null)

  const resize = (next: number) => onResize(Math.max(min, Math.round(next)))

  const onpointerdown = (e: PointerEvent) => {
    if (e.button !== 0) {
      return
    }
    // Neither select header text nor start a sort.
    e.preventDefault()
    e.stopPropagation()
    const handle = e.currentTarget as HTMLElement
    handle.setPointerCapture(e.pointerId)
    // Start from the width the column is drawn at, which for a column that fills the leftover
    // room is fractional, so the edge stays under the pointer.
    const startWidth = handle.closest('th')?.getBoundingClientRect().width ?? width
    drag = { pointerId: e.pointerId, startX: e.clientX, startWidth }
    // The pointer strays off the handle while dragging; keep the resize cursor everywhere.
    document.body.style.cursor = 'col-resize'
  }
  const onpointermove = (e: PointerEvent) => {
    if (drag?.pointerId === e.pointerId) {
      resize(drag.startWidth + e.clientX - drag.startX)
    }
  }
  const endDrag = (e: PointerEvent) => {
    if (drag?.pointerId === e.pointerId) {
      drag = null
      document.body.style.cursor = ''
    }
  }
  const onkeydown = (e: KeyboardEvent) => {
    const step = e.key === 'ArrowLeft' ? -KEYBOARD_STEP : e.key === 'ArrowRight' ? KEYBOARD_STEP : 0
    if (step) {
      e.preventDefault()
      resize(width + step)
    }
  }
</script>

<!-- A focusable separator is the ARIA pattern for a resize handle. The 8px hit area sits
  inside the cell's right edge, with the visible 1px line at that edge. Svelte counts every
  separator as non-interactive, though a focusable one is a widget. -->
<!-- svelte-ignore a11y_no_noninteractive_tabindex, a11y_no_noninteractive_element_interactions -->
<div
  role="separator"
  aria-orientation="vertical"
  aria-label="Resize {label} column"
  aria-valuenow={width}
  aria-valuemin={min}
  tabindex="0"
  class="group/resize absolute top-0 right-0 z-10 flex h-full w-2 cursor-col-resize touch-none justify-end outline-none"
  {onpointerdown}
  {onpointermove}
  onpointerup={endDrag}
  onpointercancel={endDrag}
  onclick={(e) => e.stopPropagation()}
  ondblclick={(e) => {
    e.stopPropagation()
    onReset()
  }}
  {onkeydown}
>
  <div
    class="my-2 w-px transition-colors {drag
      ? 'bg-secondary-500'
      : 'bg-transparent group-hover/col:bg-surface-200-800 group-hover/resize:bg-secondary-500 group-focus-visible/resize:bg-secondary-500'}"
  ></div>
</div>
