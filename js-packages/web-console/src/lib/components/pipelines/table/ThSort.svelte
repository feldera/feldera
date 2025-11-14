<script lang="ts" generics="T">
  import type { Field, TableHandlerInterface } from '@vincjo/datatables'
  import type { Snippet } from '$lib/types/svelte'

  let {
    table,
    field,
    children,
    class: _class
  }: {
    table: TableHandlerInterface<T>
    field: Field<T>
    children: Snippet
    class?: string
  } = $props()
  const sort = table.createSort(field)
  export {}
</script>

<th onclick={() => sort.set()} class={_class} class:active={sort.isActive}>
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
