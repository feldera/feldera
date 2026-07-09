<script lang="ts">
  // Shared presentational bar row: a flex row of per-worker bars, each with a hover tooltip.
  // Heights and colors are computed by the caller (via `colors.barPx` / `barColor`); this
  // component owns only the markup, the expand/collapse height transition, and optional
  // click-to-select. Used by both the K1 distribution bars (BarChartRow, non-interactive, grid
  // child) and the cache tile's value bars (ValueBars, selectable).
  import { Tooltip } from 'common-ui'

  interface Bar {
    /** Bar height in px (0 hides the bar; heights animate on change). */
    height: number
    /** CSS fill color. */
    color: string
    /** Tooltip text shown on hover. */
    tooltip: string
  }
  interface Props {
    bars: Bar[]
    /** Container height in px; also animated, so collapsing to 0 slides the bars away. */
    height: number
    /** Extra classes on the container (e.g. grid `col-span-*`). */
    class?: string
    /** When `onselect` is given, bars become clickable buttons and the selected one is outlined. */
    selected?: number | null
    onselect?: (worker: number) => void
  }
  const { bars, height, class: klass = '', selected = null, onselect }: Props = $props()
</script>

<div class="bar-row flex items-end gap-0.5 {klass}" style:height="{height}px">
  {#each bars as b, i (i)}
    {#if onselect}
      <button
        type="button"
        class="min-w-0 flex-1 rounded-sm transition-[height,background-color] duration-200 ease-in-out"
        class:selected={selected === i}
        style:height="{b.height}px"
        style:background-color={b.color}
        onclick={() => onselect(i)}
        aria-label={`Worker ${i}`}
      ></button>
    {:else}
      <div
        class="min-w-0 flex-1 rounded-sm transition-[height,background-color] duration-200 ease-in-out"
        style:height="{b.height}px"
        style:background-color={b.color}
      ></div>
    {/if}
    <Tooltip class="whitespace-nowrap" placement="top">{b.tooltip}</Tooltip>
  {/each}
</div>

<style>
  .bar-row {
    transition: height 200ms ease;
  }
  .selected {
    outline: 2px solid var(--color-primary-500);
    outline-offset: 1px;
  }
</style>
