<script lang="ts" generics="T">
  import type { Field, TableHandlerInterface } from '@vincjo/datatables'
  import ArrowDown from '$assets/icons/generic/arrow-down.svg?component'
  import ArrowUp from '$assets/icons/generic/arrow-up.svg?component'
  import type { Snippet } from '$lib/types/svelte'

  const {
    table,
    field,
    direction,
    onSort,
    children,
    class: _class
  }: {
    table: TableHandlerInterface<T>
    field: Field<T>
    /** Initial sort direction, applied once when the column mounts. */
    direction?: 'asc' | 'desc'
    /** Notifies the parent when the user changes this column's sort, e.g. to persist it. */
    onSort?: (direction: 'asc' | 'desc') => void
    children: Snippet
    class?: string
  } = $props()

  // Create the sort once and apply its initial direction, mirroring @vincjo/datatables' ThSort.
  // svelte-ignore state_referenced_locally
  const sort = table.createSort(field).init(direction)

  const setSort = () => {
    sort.set()
    if (sort.direction === 'asc' || sort.direction === 'desc') {
      onSort?.(sort.direction)
    }
  }

  // Drives the unsorted-column preview arrow in markup rather than through a
  // `hidden`/`group-hover:flex` class pair: this codebase's compiled Tailwind
  // resolves a plain display utility and its hover-variant sibling on the same
  // element in the plain utility's favor regardless of :hover state (the same
  // failure mode `bg-inherit` hit against `hover:bg-*` above), so the arrow never
  // appeared. Tracking hover in script sidesteps that entirely.
  let hovering = $state(false)

  const pillShown = $derived(sort.isActive || hovering)

  // A flex child ignores `text-align`, so a `text-right`/`text-center` column
  // passed in via `_class` would otherwise render its label left-aligned despite
  // its body column being right- or center-aligned. Mirror that intent onto the
  // flex axis.
  const justify = $derived(
    _class?.includes('text-right')
      ? 'justify-end'
      : _class?.includes('text-center')
        ? 'justify-center'
        : 'justify-start'
  )
</script>

<th
  onclick={setSort}
  onmouseenter={() => (hovering = true)}
  onmouseleave={() => (hovering = false)}
  class="{_class} group/sort bg-inherit whitespace-nowrap select-none cursor-pointer"
>
  <div class="flex h-full items-center {justify}">
    <!-- The pill's background lives here, not on the th: the th's own `bg-inherit`
      (needed so the sticky header stays opaque while scrolling) sets the same
      `background-color` property, and a tie between two such utilities on one
      element resolves by stylesheet source order rather than :hover state,
      silently swallowing the hover color. -->
    <!-- The pill's padding and the arrow slot are always present in the box
      model, never added/removed on hover: this table uses the browser's default
      auto layout, which repicks every column's width from its content on any
      DOM change, so toggling the arrow or the pill's padding in and out of the
      layout made the whole header row jump on hover. The `-mx-2 -my-1` here
      exactly cancels the `px-2 py-1` below, so the pill's box is the same size
      whether or not it's painted, keeping the label flush with its left-aligned
      body column at rest; showing the pill just paints its background, which is
      free to bleed into that reserved, always-there space. -->
    <div
      class="-mx-2 -my-1 flex items-center gap-1 rounded-full px-2 py-1 {pillShown
        ? 'bg-surface-50-950'
        : ''}"
    >
      {@render children()}
      <!-- Always mounted so its `h-4 w-4` is always reserved; `invisible` (not
        `hidden`) hides the icon without pulling it out of the box model. -->
      <span
        class="h-4 w-4 flex-none fill-surface-600-400 {pillShown ? '' : 'invisible'}"
      >
        {#if sort.isActive && sort.direction === 'desc'}
          <ArrowDown class="h-4 w-4" />
        {:else}
          <!-- An unsorted (or ascending) column previews the ascending arrow:
            clicking it sorts ascending first (see @vincjo/datatables' SortHandler.set). -->
          <ArrowUp class="h-4 w-4" />
        {/if}
      </span>
    </div>
  </div>
</th>
