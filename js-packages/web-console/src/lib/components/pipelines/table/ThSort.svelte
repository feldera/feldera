<script lang="ts" generics="T">
  import type { Field, TableHandlerInterface } from '@vincjo/datatables'
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
</script>

<th
  onclick={setSort}
  class="{_class} group/sort cursor-pointer bg-inherit whitespace-nowrap select-none"
>
  <!-- A `justify-*` passed in through `_class` lands on the th, a table cell that
    ignores it; `[justify-content:inherit]` hands that value to the flex row. -->
  <div class="flex h-full items-center [justify-content:inherit]">
    <div
      class="-mx-3 -my-1 flex items-center gap-1 rounded-full px-3 py-1 {sort.isActive
        ? 'bg-surface-50-950'
        : 'group-hover/sort:bg-surface-50-950'}"
    >
      {@render children()}
      <span
        class="fill-surface-600-400 align-middle text-[14px] text-surface-500 {sort.isActive
          ? ''
          : 'invisible group-hover/sort:visible'}"
      >
        {#if sort.isActive && sort.direction === 'desc'}
          <div class="fd fd-arrow-down"></div>
        {:else}
          <div class="fd fd-arrow-up"></div>
        {/if}
      </span>
    </div>
  </div>
</th>
