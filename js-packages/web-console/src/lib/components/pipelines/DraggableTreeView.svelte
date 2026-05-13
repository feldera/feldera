<script lang="ts" generics="T extends { name: string }">
  // Generic tree-view with drag-and-drop reordering between folders.
  //
  // Path conventions:
  // - "" (empty string) = top level
  // - "folder/sub" = nested folder
  //
  // The consumer supplies:
  // - leafRow / folderRow snippets to render the visible content of each row
  // - rowClass to attach grid/column-layout styles to row wrappers
  // - an optional header snippet rendered before any rows (same grid container)
  //
  // Row snippets receive a context object with drag-state flags and (for
  // folders) selection + expand/collapse helpers. To make an element act as
  // a drag handle, mark it with `data-tree-drag-handle`.

  import { type DragDropState, draggable, droppable } from '@thisux/sveltednd'
  import type { Snippet } from '$lib/types/svelte'

  type FolderNode = {
    kind: 'folder'
    path: string
    name: string
    children: TreeNode[]
  }
  type TreeNode = FolderNode | { kind: 'leaf'; path: string; item: T }

  type LeafContext = { depth: number; isDragged: boolean }
  type FolderRenderContext = {
    depth: number
    isDragged: boolean
    leafCount: number
    checkState: 'none' | 'some' | 'all'
    toggleSelection: () => void
    isExpanded: boolean
    toggleExpanded: () => void
    /** Rename this folder to a new name (last path segment only). */
    renameFolder: (newName: string) => void
  }

  let {
    items,
    getFolderPath,
    selected = $bindable(),
    onMoveToFolder,
    onCreateFolderFor,
    onMoveFolder,
    leafRow,
    folderRow,
    header,
    rowClass = '',
    containerClass = '',
    dropTargetClass = '',
    dndContainer = 'tree',
    defaultExpanded = true
  }: {
    items: T[]
    getFolderPath: (item: T) => string
    selected: string[]
    /** Called when a leaf is dropped into a folder (or top level when path = ""). */
    onMoveToFolder: (leafName: string, folderPath: string) => void
    /** Called when a leaf is dropped on another leaf, creating a new folder containing both. */
    onCreateFolderFor: (aName: string, bName: string, newFolderPath: string) => void
    /** Called when a whole folder is moved. Implementations should re-path every
        leaf whose folder path equals or starts with `oldPath`. */
    onMoveFolder: (oldPath: string, newPath: string) => void
    leafRow: Snippet<[T, LeafContext]>
    folderRow: Snippet<[FolderNode, FolderRenderContext]>
    header?: Snippet
    rowClass?: string
    containerClass?: string
    /** Tailwind classes injected on the active drop target — applied both to
        a hovered row and to the active gap drop zone's line. Caller chooses
        the visual treatment (e.g. `"bg-primary-200 dark:bg-primary-800"`). */
    dropTargetClass?: string
    dndContainer?: string
    /** When false, all folders start collapsed; newly created folders are
        auto-expanded. When true (default), all folders start expanded. */
    defaultExpanded?: boolean
  } = $props()

  // `explicitState` holds paths that were explicitly toggled by the user,
  // or (when defaultExpanded=false) paths that were programmatically expanded.
  // - defaultExpanded=true : path is COLLAPSED iff it is in the set
  // - defaultExpanded=false: path is EXPANDED  iff it is in the set
  // Either way the set only mutates on user interaction or forceExpand(), so
  // it survives list refreshes without accidentally re-collapsing/re-expanding.
  let explicitState = $state(new Set<string>())
  let allKnownPaths = $derived(
    new Set(
      items.flatMap((i) => {
        const p = getFolderPath(i)
        if (!p) {
          return []
        }
        const parts = p.split('/').filter(Boolean)
        return parts.map((_, idx) => parts.slice(0, idx + 1).join('/'))
      })
    )
  )

  // Sort each folder's children: folders first, then leaves, both groups
  // sorted lexicographically by name. Applied recursively.
  const sortNode = (node: FolderNode) => {
    node.children.sort((a, b) => {
      if (a.kind !== b.kind) {
        return a.kind === 'folder' ? -1 : 1
      }
      const aName = a.kind === 'folder' ? a.name : a.item.name
      const bName = b.kind === 'folder' ? b.name : b.item.name
      return aName.localeCompare(bName)
    })
    for (const child of node.children) {
      if (child.kind === 'folder') {
        sortNode(child)
      }
    }
  }
  const tree = $derived.by(() => {
    const root: FolderNode = { kind: 'folder', path: '', name: '', children: [] }
    const folderByPath = new Map<string, FolderNode>()
    folderByPath.set('', root)
    const ensureFolder = (path: string): FolderNode => {
      let f = folderByPath.get(path)
      if (f) {
        return f
      }
      const parts = path.split('/').filter(Boolean)
      const parentPath = parts.slice(0, -1).join('/')
      const parent = ensureFolder(parentPath)
      f = { kind: 'folder', path, name: parts[parts.length - 1], children: [] }
      folderByPath.set(path, f)
      parent.children.push(f)
      return f
    }
    for (const item of items) {
      const path = getFolderPath(item).replace(/^\/+|\/+$/g, '')
      const folder = ensureFolder(path)
      folder.children.push({ kind: 'leaf', path, item })
    }
    sortNode(root)
    return root
  })

  const folderLeafNames = (node: TreeNode): string[] =>
    node.kind === 'leaf' ? [node.item.name] : node.children.flatMap(folderLeafNames)

  const selectedSet = $derived(new Set(selected))
  const folderCheckState = (node: TreeNode): 'none' | 'some' | 'all' => {
    if (node.kind === 'leaf') {
      return selectedSet.has(node.item.name) ? 'all' : 'none'
    }
    const leaves = folderLeafNames(node)
    if (leaves.length === 0) {
      return 'none'
    }
    let on = 0
    for (const n of leaves) {
      if (selectedSet.has(n)) {
        on++
      }
    }
    if (on === 0) {
      return 'none'
    }
    if (on === leaves.length) {
      return 'all'
    }
    return 'some'
  }
  const toggleFolderSelection = (node: TreeNode) => {
    if (node.kind === 'leaf') {
      return
    }
    const leaves = folderLeafNames(node)
    const state = folderCheckState(node)
    const next = new Set(selected)
    if (state === 'all') {
      for (const n of leaves) {
        next.delete(n)
      }
    } else {
      for (const n of leaves) {
        next.add(n)
      }
    }
    selected = [...next]
  }

  // DnD state. Only one of `draggedName` / `draggedFolderPath` is set at a time;
  // they distinguish leaf vs folder drags so drop handlers can pick the right
  // mutation (move leaf vs. re-path entire folder subtree).
  let draggedName = $state<string | null>(null)
  let draggedFolderPath = $state<string | null>(null)
  let draggedFolderName = '' // captured at drag start; needed to compute new path on drop
  let dragOverFolder = $state<string | null>(null)
  let dragOverPipeline = $state<string | null>(null)
  let dragOverGap = $state<string | null>(null)
  let dropHandled = false

  const isDragging = $derived(draggedName !== null || draggedFolderPath !== null)

  // Folder X cannot be moved into itself or into any of its descendants.
  const isFolderDropForbidden = (targetParentPath: string) => {
    if (draggedFolderPath === null) {
      return false
    }
    if (targetParentPath === draggedFolderPath) {
      return true
    }
    return (
      targetParentPath === draggedFolderPath || targetParentPath.startsWith(draggedFolderPath + '/')
    )
  }

  const handleLeafDragStart = (state: DragDropState<T>) => {
    draggedName = state.draggedItem.name
    draggedFolderPath = null
    dropHandled = false
  }
  const handleLeafDragEnd = (state: DragDropState<T>) => {
    const name = state.draggedItem.name
    if (!dropHandled) {
      if (getFolderPath(state.draggedItem) !== '') {
        onMoveToFolder(name, '')
      }
    }
    draggedName = null
    draggedFolderPath = null
    dragOverFolder = null
    dragOverPipeline = null
    dragOverGap = null
  }
  const handleFolderDragStart = (path: string, name: string) => () => {
    draggedFolderPath = path
    draggedFolderName = name
    draggedName = null
    dropHandled = false
  }
  const handleFolderDragEnd = (path: string, name: string) => () => {
    if (!dropHandled) {
      const parts = path.split('/').filter(Boolean)
      if (parts.length > 1) {
        onMoveFolder(path, name)
      }
    }
    draggedName = null
    draggedFolderPath = null
    dragOverFolder = null
    dragOverPipeline = null
    dragOverGap = null
  }
  // IMPORTANT: drop handlers must clear `draggedName` *synchronously* and
  // *before* invoking the optimistic data mutation.
  //
  // The mutation re-renders the tree; since the dropped item now has a
  // different folder path, its row's DOM node is reparented to a new tree
  // position. That mid-drag DOM move breaks sveltednd's onDragEnd handshake,
  // so the `onDragEnd` callback that would normally reset `draggedName`
  // never fires. The result: the row keeps the `--being-dragged` class
  // (`visibility: hidden` contents + gray background), leaving a permanent
  // ghost at either the source or destination position.
  //
  // Clearing the state synchronously here means the ghost class is already
  // off the row by the time Svelte runs the re-render triggered by the
  // optimistic update, so the moved row paints normally at its new spot.
  const finishDrag = () => {
    dropHandled = true
    draggedName = null
    draggedFolderPath = null
    dragOverFolder = null
    dragOverPipeline = null
    dragOverGap = null
  }
  // Drop on a folder body:
  // - leaf  → move leaf INTO this folder
  // - folder → nest dragged folder INSIDE this folder (skip self / descendant)
  const handleFolderDrop = (folderPath: string) => (state: DragDropState<T>) => {
    if (draggedFolderPath !== null) {
      const oldPath = draggedFolderPath
      const folderName = draggedFolderName
      const newPath = folderPath ? `${folderPath}/${folderName}` : folderName
      const forbidden =
        folderPath === oldPath || folderPath.startsWith(oldPath + '/') || newPath === oldPath
      finishDrag()
      if (!forbidden) {
        onMoveFolder(oldPath, newPath)
      }
      return
    }
    const name = state.draggedItem.name
    const willMove = getFolderPath(state.draggedItem) !== folderPath
    finishDrag()
    if (willMove) {
      onMoveToFolder(name, folderPath)
    }
  }
  // Drop on a leaf row:
  // - leaf → create new folder containing both leaves
  // - folder → no-op (folders can only be dropped on folders or gaps)
  const handleLeafDrop = (target: T) => (state: DragDropState<T>) => {
    if (draggedFolderPath !== null) {
      finishDrag()
      return
    }
    if (state.draggedItem.name === target.name) {
      finishDrag()
      return
    }
    const targetPath = getFolderPath(target)
    const base = targetPath ? `${targetPath}/` : ''
    let folderName = 'New Group'
    let i = 2
    while (allKnownPaths.has(`${base}${folderName}`)) {
      folderName = `New Group ${i++}`
    }
    const aName = state.draggedItem.name
    const bName = target.name
    const newPath = `${base}${folderName}`
    finishDrag()
    onCreateFolderFor(aName, bName, newPath)
    forceExpand(newPath)
  }
  // Drop on a gap between rows: move to that level (the gap's parent path).
  const handleGapDrop = (parentPath: string) => (state: DragDropState<T>) => {
    if (draggedFolderPath !== null) {
      const oldPath = draggedFolderPath
      const folderName = draggedFolderName
      const newPath = parentPath ? `${parentPath}/${folderName}` : folderName
      const forbidden = isFolderDropForbidden(parentPath) || newPath === oldPath
      finishDrag()
      if (!forbidden) {
        onMoveFolder(oldPath, newPath)
      }
      return
    }
    const name = state.draggedItem.name
    const willMove = getFolderPath(state.draggedItem) !== parentPath
    finishDrag()
    if (willMove) {
      onMoveToFolder(name, parentPath)
    }
  }

  // While a drag is in flight, sveltednd runs a rAF loop that auto-scrolls
  // the viewport whenever the pointer sits within EDGE_THRESHOLD pixels of
  // any window edge — it does this by calling `window.scrollBy({ top, left })`.
  // For our tree we only want vertical assist; horizontal viewport scrolling
  // is disorienting when the table is wider than the viewport.
  $effect(() => {
    if (!isDragging) {
      return
    }
    const original = window.scrollBy.bind(window)
    type ScrollByArgs = [ScrollToOptions] | [number, number]
    ;(window as Window).scrollBy = ((...args: ScrollByArgs) => {
      if (args.length === 1 && typeof args[0] === 'object') {
        original({ ...args[0], left: 0 })
      } else if (args.length >= 2) {
        original(0, args[1] as number)
      }
    }) as typeof window.scrollBy
    return () => {
      window.scrollBy = original
    }
  })

  const isExpanded = (path: string) =>
    defaultExpanded ? !explicitState.has(path) : explicitState.has(path)

  const toggleExpanded = (path: string) => {
    const next = new Set(explicitState)
    if (next.has(path)) {
      next.delete(path)
    } else {
      next.add(path)
    }
    explicitState = next
  }

  // Ensures `path` and all its ancestors are expanded. Only meaningful when
  // defaultExpanded=false (when true, folders are open by default).
  const forceExpand = (path: string) => {
    if (defaultExpanded) {
      return
    }
    const parts = path.split('/').filter(Boolean)
    const toAdd = parts
      .map((_, i) => parts.slice(0, i + 1).join('/'))
      .filter((p) => !explicitState.has(p))
    if (toAdd.length === 0) {
      return
    }
    explicitState = new Set([...explicitState, ...toAdd])
  }
</script>

{#snippet gap(parentPath: string, key: string, depth: number)}
  {#if isDragging}
    {@const active = dragOverGap === key}
    <div
      class="tree-gap"
      style="--tree-gap-indent: {depth * 1.25}rem"
      use:droppable={{
        container: `${dndContainer}:gap:${key}`,
        callbacks: {
          onDragEnter: () => {
            if (!isDragging) return
            dragOverGap = key
            dragOverFolder = null
            dragOverPipeline = null
          },
          onDragLeave: () => {
            if (dragOverGap === key) dragOverGap = null
          },
          onDrop: handleGapDrop(parentPath)
        }
      }}
    >
      <span class="tree-gap__line {active ? dropTargetClass : ''}"></span>
    </div>
  {/if}
{/snippet}

{#snippet renderNode(node: TreeNode, depth: number)}
  {#if node.kind === 'folder'}
    {@const dragOver = dragOverFolder === node.path}
    {@const isDragged = draggedFolderPath === node.path}
    <div
      class="tree-row tree-row--folder {rowClass} {dragOver ? dropTargetClass : ''} {isDragged
        ? 'tree-row--being-dragged'
        : ''}"
      data-testid="box-folder-{node.path}"
      use:draggable={{
        dragData: { kind: 'folder', path: node.path, name: node.name },
        container: `${dndContainer}:folder-drag`,
        handle: '[data-tree-drag-handle]',
        callbacks: {
          onDragStart: handleFolderDragStart(node.path, node.name),
          onDragEnd: handleFolderDragEnd(node.path, node.name)
        }
      }}
      use:droppable={{
        container: `${dndContainer}:folder:${node.path}`,
        callbacks: {
          onDragEnter: () => {
            if (!isDragging) return
            dragOverFolder = node.path
            dragOverGap = null
          },
          onDragLeave: () => {
            if (dragOverFolder === node.path) dragOverFolder = null
          },
          onDrop: handleFolderDrop(node.path)
        }
      }}
    >
      {@render folderRow(node, {
        depth,
        isDragged,
        leafCount: folderLeafNames(node).length,
        checkState: folderCheckState(node),
        toggleSelection: () => toggleFolderSelection(node),
        isExpanded: isExpanded(node.path),
        toggleExpanded: () => toggleExpanded(node.path),
        renameFolder: (newName: string) => {
          const trimmed = newName.trim()
          if (!trimmed || trimmed === node.name) return
          const parentPath = node.path.split('/').slice(0, -1).join('/')
          const newPath = parentPath ? `${parentPath}/${trimmed}` : trimmed
          onMoveFolder(node.path, newPath)
        }
      })}
    </div>
    {#if isExpanded(node.path)}
      {#each node.children as child, i (child.kind === 'folder' ? `f:${child.path}` : `l:${child.item.name}`)}
        {@render gap(node.path, `${node.path}:${i}`, depth + 1)}
        {@render renderNode(child, depth + 1)}
      {/each}
      {@render gap(node.path, `${node.path}:end`, depth + 1)}
    {/if}
  {:else}
    {@const isDragged = draggedName === node.item.name}
    {@const dragOver = dragOverPipeline === node.item.name}
    <div
      class="tree-row {rowClass} {dragOver ? dropTargetClass : ''} {isDragged
        ? 'tree-row--being-dragged'
        : ''}"
      data-testid="box-row-{node.item.name}"
      style="--tree-indent: {depth * 1.25}rem"
      use:draggable={{
        dragData: node.item,
        container: `${dndContainer}:leaf`,
        handle: '[data-tree-drag-handle]',
        callbacks: {
          onDragStart: handleLeafDragStart,
          onDragEnd: handleLeafDragEnd
        }
      }}
      use:droppable={{
        container: `${dndContainer}:onto:${node.item.name}`,
        callbacks: {
          onDragEnter: () => {
            if (!isDragging) return
            dragOverPipeline = node.item.name
            dragOverGap = null
          },
          onDragLeave: () => {
            if (dragOverPipeline === node.item.name) dragOverPipeline = null
          },
          onDrop: handleLeafDrop(node.item)
        }
      }}
    >
      {@render leafRow(node.item, { depth, isDragged })}
    </div>
  {/if}
{/snippet}

<div class={containerClass}>
  {@render header?.()}
  {#each tree.children as child, i (child.kind === 'folder' ? `f:${child.path}` : `l:${child.item.name}`)}
    {@render gap('', `:${i}`, 0)}
    {@render renderNode(child, 0)}
  {/each}
  {@render gap('', ':end', 0)}
</div>

<style>
  /* Hide the dragged row's content but keep the element in the DOM —
     removing the source element of an HTML5 drag cancels the operation
     and prevents `drop` from firing. */
  :global(.tree-row--being-dragged) > :global(*) {
    visibility: hidden;
  }
  :global(.tree-row--being-dragged) {
    background-color: color-mix(in srgb, currentColor 6%, transparent);
  }
  /* Gap drop zone: sits between rows, overlaps their borders so it
     adds no vertical space when idle. The "insert here" line only shows
     a tint while a drag is hovering this zone (via `dropTargetClass`
     applied to the inner span). */
  :global(.tree-gap) {
    height: 16px;
    margin: -8px 0;
    position: relative;
    z-index: 2;
    pointer-events: auto;
    /* If the parent container is a CSS grid (e.g. when rows use subgrid),
       the gap row needs to span every column track. Harmless in flex/block
       parents — `grid-column` only applies to grid items. */
    grid-column: 1 / -1;
  }
  :global(.tree-gap__line) {
    position: absolute;
    right: 0;
    left: var(--tree-gap-indent, 0);
    top: 50%;
    height: 2px;
    transform: translateY(-50%);
    border-radius: 1px;
    pointer-events: none;
  }
  /* sveltednd auto-adds .drop-before/.drop-after on hovered droppables,
     drawing its own ::before/::after blue lines. We render our own
     blue line only inside the gap drop zone, so suppress the library's
     default lines on both row and gap droppables. */
  :global(.tree-row.drop-before::before),
  :global(.tree-row.drop-after::after),
  :global(.tree-gap.drop-before::before),
  :global(.tree-gap.drop-after::after) {
    display: none !important;
  }
  /* To stop the browser's default touch gestures (page scroll, pinch-zoom)
     from interfering with a drag, sveltednd writes `style.touchAction = 'none'`
     as an inline style on every `use:draggable` element. Since each row is
     a draggable, this also kills page panning anywhere on a row — even
     though we only need the lock on the grab handle.
     This override restores `touch-action: auto` on the row body so the page
     pans normally, while re-asserting `touch-action: none` on the handle so
     touching it still initiates a drag instead of scrolling. */
  :global(.tree-row) {
    touch-action: auto !important;
  }
  :global([data-tree-drag-handle]) {
    touch-action: none !important;
  }
</style>
