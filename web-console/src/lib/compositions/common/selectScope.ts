export const selectScope = (node: HTMLElement) => {
  function handleUserSelectContain(event: Event) {
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
