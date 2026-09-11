/**
 * Keeps a text selection contained within one element.
 *
 * When the user drags out of `node`, the browser extends the selection into whatever markup
 * surrounds it. This snaps the selection back to the element's own children, so a drag inside a
 * results table or an error pane cannot swallow the chrome around it.
 *
 * Done in script rather than with `user-select: contain`, which only Firefox implements.
 */
export const selectScope = (node: HTMLElement) => {
  const containSelection = () => {
    if (document.activeElement !== node) {
      return
    }

    const selection = window.getSelection()
    if (!selection || !selection.rangeCount || selection.type !== 'Range') {
      return
    }

    if (node.contains(selection.getRangeAt(0).commonAncestorContainer)) {
      return
    }

    selection.selectAllChildren(node)
  }

  document.addEventListener('selectionchange', containSelection)
  return {
    destroy() {
      document.removeEventListener('selectionchange', containSelection)
    }
  }
}
