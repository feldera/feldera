import invariant from 'tiny-invariant'

export const selectScope = (
  _node: HTMLElement,
  props?: { getNode?: (node: HTMLElement) => HTMLElement }
) => {
  function handleUserSelectContain(event: Event) {
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

export const injectOnCopyAll = (node: HTMLElement, props: { value: string }) => {}

const previousSiblingsTextLength = (node: Node | null) => {
  let textLen = 0
  while ((node = node?.previousSibling ?? null)) {
    if (node.nodeType === Node.TEXT_NODE) {
      textLen += node.textContent?.length ?? 0
    } else if (node.nodeType === Node.ELEMENT_NODE && node instanceof HTMLElement) {
      textLen += node.innerText.length
    }
  }
  return textLen
}

type RowLocation = {
  row: number
  col: number
}

type Location = {
  index: number[] // [firstChild, ...restChildren] firstChild - index in the list of .children, restChildren - indices in the list of .childNodes
  offsetLocal: number // Location in the leaf Node.TEXT_NODE
  offsetAbsolute: number // Location in the root.item(i).innerText
}

/**
 * @param parent A valid parent for the passed node
 */
const getElementLocation = (
  parent: Element,
  node: Node,
  suffix: Location = { index: <number[]>[], offsetLocal: 0, offsetAbsolute: 0 }
): Location | null => {
  if (!node.parentNode) {
    return null
  }
  const index = Array.from<Node>(
    parent === node.parentNode ? node.parentNode.children : node.parentNode.childNodes
  ).indexOf(node)
  if (index === -1) {
    return getElementLocation(parent, node.parentNode, {
      index: [0, ...suffix.index],
      offsetLocal: 0 + suffix.offsetLocal,
      offsetAbsolute: 0 + suffix.offsetAbsolute
    })
  }
  const location = {
    index: [index, ...suffix.index],
    offsetLocal: suffix.offsetLocal,
    offsetAbsolute: suffix.offsetAbsolute
  }
  if (parent === node.parentNode) {
    return location
  }
  // We do not account for previous siblings text length in the
  location.offsetAbsolute += previousSiblingsTextLength(node)
  return getElementLocation(parent, node.parentNode, location)
}

const getChildElementAtPath = (parent: Element, path: number[], parentOffset = 0) => {
  let element: Element | null = parent
  for (const i of path) {
    element =
      (element === parent
        ? element.children.item(i + parentOffset)
        : (element?.childNodes.item(i) as Element)) ?? null
  }
  return element
}

export const virtualSelect = (
  node: HTMLElement,
  {
    getRootChildrenOffset = () => 0,
    getRoot = () => node,
    getCopyContent
  }: {
    getRoot?: (node: Element) => Element
    getRootChildrenOffset?: (root: Element) => number
    getCopyContent: (slice: { start: RowLocation; end: RowLocation } | 'all') => string
  }
) => {
  let currentSelection:
    | {
        anchor: Location
        focus: Location
      }
    | null
    | 'all' = null
  const root = getRoot(node)
  function handleUserSelectContain(event: Event) {
    if (document.activeElement !== node) {
      return
    }

    const selection = window.getSelection()
    if (!selection || !selection.rangeCount || selection.type !== 'Range') {
      const minIndex = getRootChildrenOffset(root)
      const maxIndex = minIndex + root.children.length - 1
      if (
        selection &&
        selection.type === 'Caret' &&
        currentSelection &&
        typeof currentSelection === 'object' &&
        ((currentSelection.anchor.index[0] < minIndex &&
          currentSelection.focus.index[0] < minIndex) ||
          (currentSelection.anchor.index[0] > maxIndex &&
            currentSelection.focus.index[0] > maxIndex))
      ) {
        return
      }
      currentSelection = null
      return
    }
    invariant(selection.anchorNode && selection.focusNode, 'a')

    if (selection.anchorNode === node) {
      currentSelection = 'all'
      return
    }

    const anchor = getElementLocation(root, selection.anchorNode)
    const focus = getElementLocation(root, selection.focusNode)
    if (!anchor || !focus) {
      return
    }
    {
      const rootChildrenOffset = getRootChildrenOffset(root)
      anchor.index[0] += rootChildrenOffset
      focus.index[0] += rootChildrenOffset
    }
    anchor.offsetLocal += selection.anchorOffset
    focus.offsetLocal += selection.focusOffset
    anchor.offsetAbsolute += selection.anchorOffset
    focus.offsetAbsolute += selection.focusOffset
    currentSelection = {
      anchor,
      focus
    }
  }

  // As a workaround returns the first position in an element, not last, but it is not important since
  const getLastLocationInElement = (element: Element, parentIndex: number): Location | null => {
    return { index: [parentIndex], offsetLocal: 0, offsetAbsolute: 0 }
  }

  function reapplySelection() {
    const selection = window.getSelection()
    if (!selection) {
      return
    }
    if (currentSelection === null || currentSelection === 'all') {
      return
    }
    document.removeEventListener('selectionchange', handleUserSelectContain)
    invariant(selection)
    selection.removeAllRanges()
    const minIndex = getRootChildrenOffset(root)
    const maxIndex = minIndex + root.children.length - 1
    const getCroppedLocation = (index: number): Location | null => {
      return index < minIndex
        ? { index: [minIndex], offsetLocal: 0, offsetAbsolute: 0 }
        : index > maxIndex
          ? getLastLocationInElement(root.children.item(maxIndex)!, maxIndex)
          : null
    }

    let anchor = getCroppedLocation(currentSelection.anchor.index[0])
    let focus = getCroppedLocation(currentSelection.focus.index[0])

    if (
      anchor &&
      focus &&
      anchor.index[0] === focus.index[0] &&
      anchor.offsetAbsolute === focus.offsetAbsolute
    ) {
      setTimeout(() => document.addEventListener('selectionchange', handleUserSelectContain))
      // selection.setBaseAndExtent(root.children.item(0)!, 0, root.children.item(0)!, 0)
      const range = document.createRange()
      range.setStart(root.children.item(0)!, 0)
      range.setEnd(root.children.item(0)!, 0)
      selection.addRange(range)
      return
    }
    anchor ??= currentSelection.anchor
    focus ??= currentSelection.focus

    selection.setBaseAndExtent(
      getChildElementAtPath(root, anchor.index, -minIndex)!,
      anchor.offsetLocal,
      getChildElementAtPath(root, focus.index, -minIndex)!,
      focus.offsetLocal
    )
    // document.addEventListener('selectionchange', handleUserSelectContain)
    setTimeout(() => document.addEventListener('selectionchange', handleUserSelectContain))
  }

  const getRowLocation = (location: Location) => ({
    row: location.index[0],
    col: location.offsetAbsolute
  })

  function oncopy(e: ClipboardEvent) {
    if (currentSelection === null) {
      return
    }
    let content: string
    if (currentSelection === 'all') {
      content = getCopyContent('all')
    } else {
      const flip =
        currentSelection.anchor.index[0] > currentSelection.focus.index[0] ||
        (currentSelection.anchor.index[0] === currentSelection.focus.index[0] &&
          currentSelection.anchor.offsetAbsolute > currentSelection.focus.offsetAbsolute)
      content = getCopyContent({
        start: getRowLocation(flip ? currentSelection.focus : currentSelection.anchor),
        end: getRowLocation(flip ? currentSelection.anchor : currentSelection.focus)
      })
      content = getCopyContent({
        [flip ? 'end' : 'start']: getRowLocation(currentSelection.anchor),
        [flip ? 'start' : 'end']: getRowLocation(currentSelection.focus)
      } as {
        start: RowLocation
        end: RowLocation
      })
    }
    e.clipboardData!.setData('text/plain', content)
    e.preventDefault()
  }

  const mutationObserver = new MutationObserver(() => {
    setTimeout(() => reapplySelection(), 100)
  })
  mutationObserver.observe(root, { childList: true })
  document.addEventListener('selectionchange', handleUserSelectContain)
  node.addEventListener('copy', oncopy)
  return {
    destroy() {
      document.removeEventListener('selectionchange', handleUserSelectContain)
      node.removeEventListener('copy', oncopy)
    }
  }
}
