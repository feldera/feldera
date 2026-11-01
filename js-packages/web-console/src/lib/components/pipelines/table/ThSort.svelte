<script lang="ts" generics="T">
  import type { Field, TableHandlerInterface } from '@vincjo/datatables'
  import type { Snippet } from '#lib/types/svelte.ts'

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
    ignores it; `[justify-content:inherit]` hands that value to the flex row. The row claims no
    width of its own (`w-0 min-w-full`), so a fixed-layout table can make the column narrower
    than its label, which then truncates. -->
  <div class="flex h-full w-0 min-w-full items-center [justify-content:inherit]">
    <div
      class="-mx-3 -my-1 flex min-w-0 items-center gap-1 rounded-full px-3 py-1 group-hover/sort:bg-row-hover"
    >
      {@render children()}
      <span
        class="shrink-0 align-middle text-[14px] text-surface-500 {sort.isActive &&
        sort.direction === 'desc'
          ? 'fd fd-arrow-down'
          : 'fd fd-arrow-up'} {sort.isActive ? '' : 'invisible group-hover/sort:visible'}"
      ></span>
    </div>
  </div>
</th>
