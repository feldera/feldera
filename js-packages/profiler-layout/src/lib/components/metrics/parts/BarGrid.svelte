<script lang="ts" module>
  // The bar-chart column template, shared so a single-metric widget and the aggregated
  // distribution block align identically (fixed columns + a flexible label column).
  export const BAR_GRID_TEMPLATE = 'minmax(8rem, 1fr) 4rem 4rem 4rem 4.5rem'
</script>

<script lang="ts">
  import type { Snippet } from 'svelte'

  interface Props {
    /** Render the sticky Avg / Min / Max header row. */
    showHeader?: boolean
    /** First header cell (e.g. a category name); omitted for a single metric. */
    title?: string
    /** BarChartRow items filling the grid. */
    children: Snippet
  }
  const { showHeader = false, title, children }: Props = $props()

  // Sticky header cells: the box-shadow paints `--header-bg` outward to cover the grid gaps
  // (gap-x 0.75rem, gap-y 0.5rem) so scrolling rows below remain hidden. Height + leading
  // force uniform header height regardless of intrinsic font size of each cell.
  const blockHeader =
    'sticky -top-2 z-[1] mb-2 h-5 leading-5 shadow-[0_0_0_0.5rem_var(--header-bg)]'
</script>

<div
  class="grid min-w-96 items-baseline gap-x-3 gap-y-2 pb-2"
  style="grid-template-columns: {BAR_GRID_TEMPLATE};"
>
  {#if showHeader}
    <!-- The rightmost (skew) column has no header so the right edge is reserved for the per-row
         skew toggle. The negative top/horizontal margins + padding extend the background out to
         cover the card's own padding when sticking. -->
    {#if title}
      <h3 class={`${blockHeader} bg-white-dark text-base font-semibold text-surface-900-100`}>
        {title}
      </h3>
    {:else}
      <span class={`${blockHeader} bg-white-dark`}></span>
    {/if}
    <div class={`${blockHeader} bg-white-dark text-right font-medium`}>Avg</div>
    <div class={`${blockHeader} bg-white-dark text-right font-medium`}>Min</div>
    <div class={`${blockHeader} bg-white-dark text-right font-medium`}>Max</div>
    <div class={`${blockHeader} bg-white-dark`}></div>
  {/if}
  {@render children()}
</div>
