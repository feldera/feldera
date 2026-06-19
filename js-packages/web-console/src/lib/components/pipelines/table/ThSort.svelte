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

<th onclick={setSort} class={_class} class:active={sort.isActive}>
  <div class="flex">
    {@render children()}
    <span class:asc={sort.direction === 'asc'} class:desc={sort.direction === 'desc'}> </span>
  </div>
</th>

<style>
  th {
    background: inherit;
    white-space: nowrap;
    user-select: none;
    /* border-bottom: 1px solid var(--grey, #e0e0e0); */
    cursor: pointer;
  }
  div.flex {
    padding: 0;
    display: flex;
    align-items: center;
    justify-content: flex-start;
    height: 100%;
  }
  span {
    padding-left: 8px;
  }
  span:before,
  span:after {
    border: 6px solid transparent;
    content: '';
    display: block;
    height: 0;
    width: 0;
  }
  span:before {
    border-bottom-color: var(--grey, #e0e0e0);
    margin-top: 4px;
  }
  span:after {
    border-top-color: var(--grey, #e0e0e0);
    margin-top: 4px;
  }
  th.active span.asc:before {
    border-bottom-color: var(--font-grey, #9e9e9e);
  }
  th.active span.desc:after {
    border-top-color: var(--font-grey, #9e9e9e);
  }
</style>
