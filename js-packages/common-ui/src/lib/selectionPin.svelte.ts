/**
 * Keeps the chunks a selection touches mounted, so selecting text and scrolling at the same time
 * does not destroy the selection.
 *
 * A virtualiser unmounts what leaves the viewport, and a selection whose endpoints have been
 * removed from the document collapses. The fix is to stop unmounting the parts the user has
 * selected: every chunk the live selection intersects is fed to the virtualiser's `keepMounted`
 * and held there until the selection collapses. GitHub's log viewer does the same thing with a
 * `containsNode` check before each unmount.
 *
 * The set only ever grows while a selection is live. Shedding chunks behind the cursor would undo
 * the point of it: a drag accumulates the full range precisely because nothing already selected is
 * released.
 *
 * Select-all is the exception, and it has to be, because pinning does not scale to it: a drag is
 * bounded by how far the user can reach, but Ctrl+A claims the whole log at once. Pinning every
 * chunk it touches would mount the entire log and undo the virtualisation. So a select-all pins
 * nothing and is reported through {@link SelectionPin.selectAll} instead, which the copy path
 * turns into a whole-log slice read straight from the source array. Nothing is lost: the source is
 * already in memory, so a copy never needed the DOM to be complete.
 */

/** Chunk keys currently held in the DOM on the selection's behalf, plus the select-all escape. */
export type SelectionPin = {
  readonly pinned: number[]
  /**
   * True while a select-all is in force, meaning nothing is pinned and the whole log is selected.
   * Copy must read the source rather than resolve DOM endpoints, which would see only the rows
   * that happen to be mounted.
   */
  readonly selectAll: boolean
  /**
   * True while the user is holding a selection that re-rendering the rows would collapse.
   *
   * Probes the live selection rather than reporting {@link SelectionPin.pinned}, which is recorded
   * from a `selectionchange` event and so lags it by a task. A rebuild landing inside that window
   * is exactly the one that destroys a selection just made.
   */
  isHeld(): boolean
}

const isSelectAllShortcut = (event: KeyboardEvent) =>
  (event.ctrlKey || event.metaKey) &&
  !event.altKey &&
  !event.shiftKey &&
  event.key.toLowerCase() === 'a'

export const useSelectionPin = (getRoot: () => HTMLElement | undefined): SelectionPin => {
  let pinned = $state<number[]>([])
  let selectAll = $state(false)
  const held = new Set<number>()

  const releasePins = () => {
    if (held.size > 0) {
      held.clear()
      pinned = []
    }
  }

  const onSelectionChange = () => {
    const root = getRoot()
    const selection = document.getSelection()

    // A select-all holds until the user starts a new selection, and deliberately survives the
    // selection collapsing under it. Scrolling unmounts the rows the endpoints sit on, which in
    // some browsers collapses the range; treating that as "the user deselected" would lose the
    // whole-log copy that Ctrl+A was for.
    if (selectAll) {
      return
    }

    if (!root || !selection || selection.isCollapsed || selection.rangeCount === 0) {
      releasePins()
      return
    }

    // Only chunks currently in the document can be tested. Ones already held stay held, which is
    // what accumulates the full range across a drag that scrolls.
    const before = held.size
    for (const el of root.querySelectorAll<HTMLElement>('[data-chunk]')) {
      if (selection.containsNode(el, true)) {
        held.add(Number(el.dataset.chunk))
      }
    }
    if (held.size !== before) {
      pinned = [...held]
    }
  }

  // Listened for on the document rather than the container so a select-all still registers when
  // focus sits on a child, then gated on the log actually owning the selection — otherwise a
  // Ctrl+A meant for a search input elsewhere on the page would silently drop our pins.
  const onKeyDown = (event: KeyboardEvent) => {
    if (!isSelectAllShortcut(event)) {
      return
    }
    const root = getRoot()
    if (!root) {
      return
    }
    const active = document.activeElement
    if (!active || !root.contains(active)) {
      return
    }
    selectAll = true
    releasePins()
  }

  // A pointer press begins a fresh selection, which is the one unambiguous signal that the
  // select-all is over. Normal pinning resumes from the next selectionchange.
  const onPointerDown = () => {
    selectAll = false
  }

  $effect(() => {
    document.addEventListener('selectionchange', onSelectionChange)
    document.addEventListener('keydown', onKeyDown)
    document.addEventListener('pointerdown', onPointerDown)
    return () => {
      document.removeEventListener('selectionchange', onSelectionChange)
      document.removeEventListener('keydown', onKeyDown)
      document.removeEventListener('pointerdown', onPointerDown)
    }
  })

  const isHeld = () => {
    if (selectAll) {
      return true
    }
    const root = getRoot()
    const selection = document.getSelection()
    if (!root || !selection || selection.isCollapsed || selection.rangeCount === 0) {
      return false
    }
    return root.contains(selection.getRangeAt(0).commonAncestorContainer)
  }

  return {
    get pinned() {
      return pinned
    },
    get selectAll() {
      return selectAll
    },
    isHeld
  }
}
