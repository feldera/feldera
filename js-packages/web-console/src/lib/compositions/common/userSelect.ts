import invariant from 'tiny-invariant'

export const selectScope = (
  _node: HTMLElement,
  props?: { getNode?: (node: HTMLElement) => HTMLElement }
) => {
  function handleUserSelectContain() {
    const node = props?.getNode?.(_node) ?? _node
    if (document.activeElement !== node) {
      return
    }

    const selection = window.getSelection()
    if (!selection || !selection.rangeCount || selection.type !== 'Range') {
      return
    }

    const container = selection.getRangeAt(0).commonAncestorContainer
    if (node.contains(container)) {
      return
    }

    selection.selectAllChildren(node)
  }
  document.addEventListener('selectionchange', handleUserSelectContain)
  return {
    destroy() {
      document.removeEventListener('selectionchange', handleUserSelectContain)
    }
  }
}

type RowLocation = {
  row: number
  col: number
}

/** Walk up the DOM to find the nearest ancestor with a `data-rowindex` attribute */
const findRowAncestor = (node: Node): HTMLElement | null => {
  let current: Node | null = node
  while (current) {
    if (current instanceof HTMLElement && current.hasAttribute('data-rowindex')) {
      return current
    }
    current = current.parentNode
  }
  return null
}

/** Get the character offset from the start of a row element to a specific DOM position */
const getCharOffset = (rowElement: HTMLElement, node: Node, offset: number): number => {
  const range = document.createRange()
  range.setStart(rowElement, 0)
  range.setEnd(node, offset)
  return range.toString().length
}

/** Convert a DOM selection point to a logical {row, col} position */
const domPointToLogical = (node: Node, offset: number): RowLocation | null => {
  const rowElement = findRowAncestor(node)
  if (!rowElement) {
    return null
  }
  const row = parseInt(rowElement.getAttribute('data-rowindex')!)
  if (isNaN(row)) {
    return null
  }
  return { row, col: getCharOffset(rowElement, node, offset) }
}

/** Iterate over row elements in the virtualizer root, calling fn for each row index and its wrapper */
const forEachRow = (root: Element, fn: (rowIndex: number, wrapper: Element) => void) => {
  for (let i = 0; i < root.children.length; i++) {
    const child = root.children[i]
    // The virtualizer root's direct children are wrappers; the data-rowindex is on their first child
    const rowEl = child.querySelector('[data-rowindex]')
    if (!rowEl) {
      continue
    }
    const idx = parseInt(rowEl.getAttribute('data-rowindex')!)
    if (!isNaN(idx)) {
      fn(idx, child)
    }
  }
}

/** Compute min/max row indices from a set of row elements, optionally filtering by a predicate */
const rowRange = (
  root: Element,
  filter?: (wrapper: Element) => boolean
): { min: number; max: number } | null => {
  let min = Infinity
  let max = -Infinity
  forEachRow(root, (idx, wrapper) => {
    if (filter && !filter(wrapper)) {
      return
    }
    if (idx < min) {
      min = idx
    }
    if (idx > max) {
      max = idx
    }
  })
  if (min === Infinity) {
    return null
  }
  return { min, max }
}

/** Get the range of row indices present in the virtualizer root's DOM (including overscan) */
const getVisibleRange = (root: Element) => rowRange(root)

/** Find the DOM element for a given row index within the root */
const findRowElement = (root: Element, rowIndex: number): HTMLElement | null => {
  return root.querySelector(`[data-rowindex="${rowIndex}"]`)
}

/** Convert a character offset within a row element to a DOM {node, offset} position */
const charOffsetToDOM = (
  rowElement: HTMLElement,
  charOffset: number
): { node: Node; offset: number } | null => {
  const walker = document.createTreeWalker(rowElement, NodeFilter.SHOW_TEXT)
  let remaining = charOffset
  let lastTextNode: Text | null = null
  let textNode: Text | null
  while ((textNode = walker.nextNode() as Text | null)) {
    const len = textNode.textContent?.length ?? 0
    if (remaining <= len) {
      return { node: textNode, offset: remaining }
    }
    remaining -= len
    lastTextNode = textNode
  }
  // Offset beyond text content — clamp to end of last text node
  if (lastTextNode) {
    return { node: lastTextNode, offset: lastTextNode.textContent?.length ?? 0 }
  }
  // No text nodes at all — return the element itself
  return { node: rowElement, offset: 0 }
}

/** Clamp a RowLocation to a visible range: col=0 for above, col=Infinity for below */
const clampToVisible = (loc: RowLocation, visible: { min: number; max: number }): RowLocation => {
  if (loc.row < visible.min) {
    return { row: visible.min, col: 0 }
  }
  if (loc.row > visible.max) {
    return { row: visible.max, col: Infinity }
  }
  return loc
}

/** Resolve a clamped col=Infinity to a DOM position at the end of the row element */
const resolveToDOM = (loc: RowLocation, rowEl: HTMLElement) =>
  loc.col === Infinity
    ? { node: rowEl as Node, offset: rowEl.childNodes.length }
    : charOffsetToDOM(rowEl, loc.col)

export const virtualSelect = (
  node: HTMLElement,
  {
    getRoot = () => node,
    getCopyContent
  }: {
    getRoot?: (node: Element) => Element
    getCopyContent: (slice: { start: RowLocation; end: RowLocation } | 'all') => string
  }
) => {
  const root = getRoot(node)

  let currentSelection: { anchor: RowLocation; focus: RowLocation } | 'all' | null = null
  let isReapplying = false
  // MutationObserver (microtask) sets this before selectionchange (task) fires,
  // so handleSelectionChange can distinguish DOM recycling from user action.
  let hadMutation = false
  // Caret position from the user's last click. Survives DOM recycling so
  // Shift+click can extend to it even after the original DOM node was recycled.
  let caretPosition: RowLocation | null = null
  // Set by pointerdown, consumed by handleSelectionChange to know when to update caretPosition.
  let expectCaretFromClick = false
  // Set when Shift+click is detected with an off-screen caretPosition. Handled in
  // handleSelectionChange to construct the selection ourselves (the browser can't
  // create the right Range because the original caret's DOM node was recycled).
  let shiftClickPending = false
  // True while the left mouse button is held on content (drag-selecting).
  // Used to suppress reapplySelection during drag — calling setBaseAndExtent while
  // the browser is tracking a drag breaks its native selection direction.
  let isDragging = false
  // Track when the mouse leaves the container during a drag-selection, so the MO
  // callback knows to extend the focus toward newly visible rows.
  let dragOutside: 'above' | 'below' | null = null
  // Set when handleSelectionChange detects that Firefox placed the anchor on the
  // container/root node (its fallback when the anchor's DOM node is recycled).
  // This signals the MO callback that the browser can't maintain the selection
  // natively, so we must reapply and extend focus ourselves even during a drag
  // inside the container. Cleared on pointerup when the drag ends.
  let browserSelectionBroken = false

  /** Like getVisibleRange but only includes rows actually within the scroll container's viewport,
   *  excluding virtua's overscan/buffer rows that are in the DOM but scrolled off-screen. */
  function getViewportVisibleRange() {
    const containerRect = node.getBoundingClientRect()
    return rowRange(root, (wrapper) => {
      const rect = wrapper.getBoundingClientRect()
      return rect.bottom > containerRect.top && rect.top < containerRect.bottom
    })
  }

  /**
   * Merge browser-reported anchor/focus with tracked state to handle off-screen endpoints.
   * When virtua recycles DOM nodes, the browser creates fallback endpoints at visible boundaries.
   * This detects those fallbacks and substitutes the tracked logical positions.
   */
  function mergeWithTracked(
    anchor: RowLocation,
    focus: RowLocation,
    visible: { min: number; max: number }
  ): { anchor: RowLocation; focus: RowLocation } {
    if (currentSelection && typeof currentSelection === 'object') {
      const tracked = currentSelection
      const anchorOffscreen = tracked.anchor.row < visible.min || tracked.anchor.row > visible.max

      if (anchorOffscreen) {
        const focusOffscreen =
          tracked.focus.row < visible.min || tracked.focus.row > visible.max
        if (focusOffscreen) {
          // Both tracked endpoints are off-screen — the browser's anchor and focus
          // are both clamped values from reapplySelection. Preserve entire tracked
          // selection; reapplySelection will clamp for display.
          return { anchor: tracked.anchor, focus: tracked.focus }
        }
        // Only the anchor is off-screen. The browser may have flipped anchor/focus
        // due to absolute positioning (DOM order ≠ visual order). Use the tracked
        // drag direction to identify which browser endpoint is the real focus
        // (the one furthest in the drag direction = the mouse position).
        const draggingDown = tracked.anchor.row <= tracked.focus.row
        const realFocus = draggingDown
          ? anchor.row >= focus.row
            ? anchor
            : focus
          : anchor.row <= focus.row
            ? anchor
            : focus
        return { anchor: tracked.anchor, focus: realFocus }
      }
      // Anchor is on-screen. If tracked focus is off-screen, preserve it — the
      // browser's focus is a clamped/fallback value, not a real user selection.
      if (tracked.focus.row < visible.min || tracked.focus.row > visible.max) {
        return { anchor, focus: tracked.focus }
      }
    }

    // Also merge with caretPosition for Shift+click: the user clicked (setting caretPosition),
    // scrolled away (caret's DOM node recycled), then Shift+clicked. The browser created a Range
    // from a visible boundary to the Shift+click point, but the real anchor is caretPosition.
    // Check either boundary — when a DOM node is removed, the browser typically collapses the
    // caret toward the start of the parent (visible.min) regardless of the original direction.
    if (
      caretPosition &&
      (caretPosition.row < visible.min || caretPosition.row > visible.max) &&
      (anchor.row === visible.min || anchor.row === visible.max)
    ) {
      return { anchor: caretPosition, focus }
    }

    return { anchor, focus }
  }

  function handleSelectionChange() {
    // Consume hadMutation before any early returns so it doesn't stay stuck true
    // when isReapplying catches all selectionchange events from a reapply cycle.
    const wasMutation = hadMutation
    hadMutation = false

    if (isReapplying) {
      return
    }
    if (document.activeElement !== node) {
      return
    }

    // Handle Shift+click with off-screen caretPosition. The browser may produce a
    // Caret, a Range with wrong anchor, or anchor on the root — we bypass all that
    // and construct the selection from caretPosition + the click position.
    if (shiftClickPending) {
      shiftClickPending = false
      const selection = window.getSelection()
      if (selection && selection.rangeCount && caretPosition) {
        // The click position is the focus for Range, or the anchor for Caret
        const clickNode = selection.type === 'Range' ? selection.focusNode : selection.anchorNode
        const clickOffset = selection.type === 'Range' ? selection.focusOffset : selection.anchorOffset
        if (!clickNode) {
          return
        }
        const clickPos = domPointToLogical(clickNode, clickOffset)
        if (!clickPos) {
          return
        }
        currentSelection = { anchor: caretPosition, focus: clickPos }
        reapplySelection()
      }
    }

    const selection = window.getSelection()
    if (!selection || !selection.rangeCount) {
      if (!wasMutation) {
        currentSelection = null
      }
      return
    }

    // Caret (collapsed selection)
    if (selection.type !== 'Range') {
      // Record caret position from user clicks (not from DOM mutations) so that
      // Shift+click can extend to it even after the original DOM node was recycled.
      if (expectCaretFromClick) {
        expectCaretFromClick = false
        if (selection.anchorNode && root.contains(selection.anchorNode)) {
          const pos = domPointToLogical(selection.anchorNode, selection.anchorOffset)
          if (pos) caretPosition = pos
        }
      }
      // Selection collapsed — decide whether to preserve the tracked selection.
      if (currentSelection && typeof currentSelection === 'object') {
        // If this collapse was triggered by a DOM mutation (virtua recycling nodes),
        // always preserve — reapplySelection already restored the visible portion.
        if (wasMutation) {
          return
        }
        // Otherwise check if any tracked endpoint is off-screen. If so, the collapse
        // is likely from scrolling (the anchor/focus node left the DOM). Preserve it
        // so reapplySelection can restore the visible portion on the next mutation.
        const visible = getVisibleRange(root)
        if (visible) {
          const { anchor, focus } = currentSelection
          if (
            anchor.row < visible.min ||
            anchor.row > visible.max ||
            focus.row < visible.min ||
            focus.row > visible.max
          ) {
            return // preserve tracked selection
          }
        }
      }
      currentSelection = null
      return
    }

    invariant(selection.anchorNode && selection.focusNode, 'Selection must have anchor and focus')

    // If anchor is on the root node itself, it could be:
    // (a) A real select-all (Ctrl+A) — no tracked selection or not dragging
    // (b) Firefox's fallback when the anchor's DOM node was recycled during a drag —
    //     Firefox places the anchor on the container/root instead of at a visible boundary
    //     like Chrome does. In this case, preserve the tracked anchor and read focus from
    //     the browser's selection.
    if (selection.anchorNode === root || selection.anchorNode === node) {
      if (currentSelection && typeof currentSelection === 'object') {
        // Firefox fallback — when the anchor's DOM node is recycled during a drag,
        // Firefox places the anchor on the container/root instead of at a visible
        // boundary like Chrome does. The focusNode may also land on root/node if
        // Firefox can't resolve either endpoint.
        //
        // When we have a tracked selection and are dragging (or the tracked anchor
        // is off-screen), preserve the tracked selection. If the browser's focusNode
        // resolves to a row, use it as the updated focus; otherwise keep the tracked
        // focus — reapplySelection will clamp it to the visible range.
        const visible = getVisibleRange(root)
        const trackedAnchorOffscreen =
          visible &&
          (currentSelection.anchor.row < visible.min || currentSelection.anchor.row > visible.max)
        if (trackedAnchorOffscreen || isDragging) {
          const browserFocus = selection.focusNode
            ? domPointToLogical(selection.focusNode, selection.focusOffset)
            : null
          const focus = browserFocus ?? currentSelection.focus
          // Signal to MO callback that the browser can't maintain the selection
          // natively — we must reapply and extend focus ourselves even during drag.
          browserSelectionBroken = true
          currentSelection = { anchor: currentSelection.anchor, focus }
          return
        }
      }
      currentSelection = 'all'
      return
    }

    // Check if selection is within our root
    if (!root.contains(selection.anchorNode)) {
      // Selection leaked outside — act like selectScope: select all children
      if (!currentSelection) {
        selection.selectAllChildren(node)
      }
      return
    }

    let anchor = domPointToLogical(selection.anchorNode, selection.anchorOffset)
    let focus = domPointToLogical(selection.focusNode, selection.focusOffset)
    if (!anchor || !focus) {
      return
    }

    // When not actively dragging and no DOM mutation triggered this event, a Range
    // selectionchange is likely a late/stale event from our own reapplySelection
    // (fired after isReapplying was cleared by setTimeout). If the tracked selection
    // has off-screen endpoints, the browser's Range contains clamped values — not
    // the real selection. Preserve the tracked selection, same as the Caret handler.
    if (!isDragging && !wasMutation && currentSelection && typeof currentSelection === 'object') {
      const visible = getVisibleRange(root)
      if (visible) {
        const { anchor: tA, focus: tF } = currentSelection
        if (
          tA.row < visible.min || tA.row > visible.max ||
          tF.row < visible.min || tF.row > visible.max
        ) {
          return
        }
      }
    }

    const beforeMerge = { anchor: { ...anchor }, focus: { ...focus } }
    expectCaretFromClick = false

    // When virtua removes a DOM node that was part of the selection (e.g. the anchor
    // scrolled off-screen during a drag), the browser creates a new Range with a
    // fallback endpoint at the visible boundary. With absolute positioning, the browser
    // may also flip anchor/focus (DOM order ≠ visual order). Merge with tracked values.
    const visible = getVisibleRange(root)
    if (visible) {
      ;({ anchor, focus } = mergeWithTracked(anchor, focus, visible))
    }

    currentSelection = { anchor, focus }
  }

  function reapplySelection() {
    if (currentSelection === null || currentSelection === 'all') {
      return
    }

    const selection = window.getSelection()
    if (!selection) {
      return
    }

    const visible = getVisibleRange(root)
    if (!visible) {
      return
    }

    const { anchor, focus } = currentSelection
    // Clamp to visible range
    const clampedAnchor = clampToVisible(anchor, visible)
    const clampedFocus = clampToVisible(focus, visible)

    // setBaseAndExtent triggers selectionchange asynchronously (task), but isReapplying
    // is cleared synchronously. Use setTimeout to keep the flag true until after the
    // pending selectionchange events are processed — this prevents handleSelectionChange
    // from overwriting the tracked selection with clamped values.
    isReapplying = true
    const endReapply = () =>
      setTimeout(() => {
        isReapplying = false
      })

    // If both clamped to same boundary with same col, set a collapsed range
    if (
      clampedAnchor.row === clampedFocus.row &&
      clampedAnchor.col === clampedFocus.col &&
      (clampedAnchor.col === Infinity || clampedAnchor.col === 0)
    ) {
      // Both off-screen on same side — just collapse at boundary
      const boundaryRow = findRowElement(root, clampedAnchor.row)
      if (!boundaryRow) {
        endReapply()
        return
      }
      selection.removeAllRanges()
      const range = document.createRange()
      range.setStart(boundaryRow, 0)
      range.setEnd(boundaryRow, 0)
      selection.addRange(range)
      endReapply()
      return
    }

    const anchorRow = findRowElement(root, clampedAnchor.row)
    const focusRow = findRowElement(root, clampedFocus.row)
    if (!anchorRow || !focusRow) {
      endReapply()
      return
    }

    const anchorDOM = resolveToDOM(clampedAnchor, anchorRow)
    const focusDOM = resolveToDOM(clampedFocus, focusRow)
    if (!anchorDOM || !focusDOM) {
      endReapply()
      return
    }

    try {
      selection.setBaseAndExtent(anchorDOM.node, anchorDOM.offset, focusDOM.node, focusDOM.offset)
    } catch {
      // setBaseAndExtent can throw if nodes were removed between check and call
    }
    endReapply()
  }

  function oncopy(e: ClipboardEvent) {
    if (currentSelection === null) return
    let content: string
    if (currentSelection === 'all') {
      content = getCopyContent('all')
    } else {
      // Normalize so start <= end regardless of selection direction
      const { anchor, focus } = currentSelection
      const flip = anchor.row > focus.row || (anchor.row === focus.row && anchor.col > focus.col)
      content = getCopyContent({
        start: flip ? focus : anchor,
        end: flip ? anchor : focus
      })
    }
    e.clipboardData!.setData('text/plain', content)
    e.preventDefault()
  }

  const mutationObserver = new MutationObserver(() => {
    // Set flag before reapply — MutationObserver callbacks (microtasks) run before
    // the selectionchange event (task) that the browser fires when DOM removal
    // collapses the selection. This lets handleSelectionChange know the collapse
    // was caused by DOM recycling, not a user action.
    hadMutation = true

    // When dragging outside the container, or when Firefox's selection is broken
    // (anchor placed on root — detected by browserSelectionBroken flag), proactively
    // extend the focus to the viewport-visible boundary in the drag direction.
    // Use getBoundingClientRect to exclude virtua's overscan/buffer rows that are in
    // the DOM but scrolled outside the viewport.
    //
    // Determine the drag direction: use dragOutside if set (pointerleave fired),
    // otherwise infer from tracked selection when browserSelectionBroken is set
    // (Firefox near-edge auto-scroll where the mouse stays inside the container).
    let effectiveDragDir: 'above' | 'below' | null = dragOutside
    if (
      !effectiveDragDir &&
      browserSelectionBroken &&
      isDragging &&
      currentSelection &&
      typeof currentSelection === 'object'
    ) {
      // Infer direction from anchor vs focus
      effectiveDragDir =
        currentSelection.anchor.row > currentSelection.focus.row ? 'above' : 'below'
    }

    if (effectiveDragDir && currentSelection && typeof currentSelection === 'object') {
      const visible = getViewportVisibleRange()
      if (visible) {
        const prevFocus = { ...currentSelection.focus }
        if (effectiveDragDir === 'above') {
          currentSelection = { ...currentSelection, focus: { row: visible.min, col: 0 } }
        } else {
          const lastRow = findRowElement(root, visible.max)
          const col = lastRow ? lastRow.innerText.length : 0
          currentSelection = {
            ...currentSelection,
            focus: { row: visible.max, col }
          }
        }
      }
    }

    // Don't call reapplySelection during an active drag inside the container —
    // setBaseAndExtent conflicts with the browser's native drag-selection tracking
    // and can invert the selection direction. The browser handles the visual selection;
    // we just track logical positions via handleSelectionChange.
    // When mouse is outside (dragOutside), we must reapply to extend to new rows.
    // When not dragging (scroll-then-view), we must reapply to restore the selection.
    //
    // Exception: when browserSelectionBroken is set (Firefox placed the anchor on
    // the container root), the browser can't maintain the selection natively and
    // we must reapply even during a drag inside the container.
    const needsReapply = !isDragging || !!dragOutside || browserSelectionBroken
    if (needsReapply) {
      reapplySelection()
    }
  })

  // Clear tracked selection on left-click. pointerdown fires before selectionchange,
  // so this ensures "click to deselect" works even when tracked endpoints are off-screen
  // (where the off-screen check in handleSelectionChange would otherwise preserve them).
  // Starting a new drag-selection is unaffected: handleSelectionChange will set
  // currentSelection when the Range appears.
  function handlePointerDown(e: PointerEvent) {
    if (e.button !== 0) return
    // Ignore clicks on the scrollbar. We use elementFromPoint to detect this:
    // if the hit element is the scroll container itself (not a descendant), the
    // click landed on the scrollbar or empty padding. This works for both Chrome's
    // classic scrollbar (which occupies space outside clientWidth) and Firefox's
    // overlay scrollbar (which hovers over content and doesn't reduce clientWidth).
    const hitEl = document.elementFromPoint(e.clientX, e.clientY)
    if (hitEl === node) return

    // Shift+click with an off-screen caretPosition: the browser can't create the
    // right Range (the original caret node was recycled), so we'll handle it in
    // handleSelectionChange by using caretPosition as the anchor.
    if (e.shiftKey && caretPosition) {
      const visible = getVisibleRange(root)
      if (visible && (caretPosition.row < visible.min || caretPosition.row > visible.max)) {
        shiftClickPending = true
        dragOutside = null
        return
      }
    }

    currentSelection = null
    dragOutside = null
    isDragging = true
    browserSelectionBroken = false
    expectCaretFromClick = true
  }

  function handlePointerLeave(e: PointerEvent) {
    if (!(e.buttons & 1)) {
      return // left button not held
    }
    const rect = node.getBoundingClientRect()
    if (e.clientY <= rect.top) {
      dragOutside = 'above'
    } else if (e.clientY >= rect.bottom) {
      dragOutside = 'below'
    }
  }

  function handlePointerEnter() {
    dragOutside = null
  }

  function handlePointerUp() {
    const wasDragging = isDragging
    isDragging = false
    dragOutside = null
    browserSelectionBroken = false
    // reapplySelection was suppressed during drag inside the container.
    // Apply once now to show the clamped selection.
    if (wasDragging) {
      reapplySelection()
    }
  }

  mutationObserver.observe(root, { childList: true })
  document.addEventListener('selectionchange', handleSelectionChange)
  document.addEventListener('pointerup', handlePointerUp)
  node.addEventListener('copy', oncopy)
  node.addEventListener('pointerdown', handlePointerDown)
  node.addEventListener('pointerleave', handlePointerLeave)
  node.addEventListener('pointerenter', handlePointerEnter)
  return {
    destroy() {
      mutationObserver.disconnect()
      document.removeEventListener('selectionchange', handleSelectionChange)
      document.removeEventListener('pointerup', handlePointerUp)
      node.removeEventListener('copy', oncopy)
      node.removeEventListener('pointerdown', handlePointerDown)
      node.removeEventListener('pointerleave', handlePointerLeave)
      node.removeEventListener('pointerenter', handlePointerEnter)
    }
  }
}
