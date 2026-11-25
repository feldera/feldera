<script lang="ts" module>
  import type { TooltipData } from './ProfilerTooltip.svelte'

  export type TopNodes = Extract<TooltipData, { genericTable: unknown }>['genericTable']
</script>

<script lang="ts">
  import { VList } from 'virtua/svelte'

  const { table }: { table: TopNodes } = $props()

  // The table has a row for each drawn node, often more than a thousand, so it mounts only the rows
  // near the view. The rows are absolutely placed, so they cannot share the column widths of a table.
  // The node and value columns are as wide as their longest text.
  let list: VList<TopNodes['rows'][number]> | undefined = $state()
  // The estimated row height is one line of `text-sm`, the `py-1` padding and the top border.
  // The list measures the real height of a taller row, for example a long operation on two lines.
  const rowHeightPx = 29
  const columns = $derived.by(() => {
    const widestCh = (texts: string[]) => Math.max(...texts.map((text) => text.length)) + 2
    const node = widestCh([table.columns[0] ?? '', ...table.rows.map((row) => row.stub.text)])
    const value = widestCh([
      table.columns[1] ?? '',
      ...table.rows.map((row) => row.cells[0]?.text ?? '')
    ])
    return `${node}ch ${value}ch minmax(0, 1fr)`
  })

  /** Scrolls the row to the top of the list. The list mounts the row only after it scrolls to it. */
  export function scrollToRow(index: number) {
    list?.scrollToIndex(index, { align: 'start', smooth: true })
  }
</script>

<div
  class="flex h-full flex-col rounded-base bg-white-dark p-4 shadow-sm text-sm"
  role="table"
  aria-label={table.header}
  aria-rowcount={table.rows.length + 1}
>
  <h3 class="mb-3 text-base font-semibold text-surface-900-100">{table.header}</h3>
  <div
    class="grid text-left text-xs font-medium uppercase tracking-wide text-surface-600-400"
    style:grid-template-columns={columns}
    role="row"
  >
    {#each table.columns as col}
      <div class="px-2 py-1" role="columnheader">{col}</div>
    {/each}
  </div>
  <VList
    class="scrollbar min-h-0 flex-1"
    role="rowgroup"
    data={table.rows}
    itemSize={rowHeightPx}
    bind:this={list}
  >
    {#snippet children(row, i)}
      <div
        class="grid border-t border-surface-200-800"
        style:grid-template-columns={columns}
        role="row"
        aria-rowindex={i + 2}
        data-block-id="top-node-{i}"
      >
        <div class="px-2 py-1" role="cell">
          <button
            type="button"
            class="cursor-pointer text-secondary-600-400 hover:underline"
            onclick={() => row.stub.onclick?.()}
          >{row.stub.text}</button>
        </div>
        {#each row.cells as cell}
          <div class="px-2 py-1 text-right tabular-nums" role="cell">{cell.text}</div>
          <div class="px-2 py-1 text-surface-700-300" role="cell">{cell.operation}</div>
        {/each}
      </div>
    {/snippet}
  </VList>
</div>
