<script lang="ts">
  import type { RenderableMetric } from '../dispatch'
  import { parseFormatted } from '../format'
  import BarChartMetric from '../parts/BarChartMetric.svelte'

  interface Props {
    id: string
    title?: string
    entries: RenderableMetric[]
  }
  const { id, title, entries }: Props = $props()

  let expandedIds = $state(new Set<string>())

  function isExpanded(row: RenderableMetric): boolean {
    return expandedIds.has(row.row.metric)
  }
  function toggle(row: RenderableMetric) {
    const next = new Set(expandedIds)
    if (next.has(row.row.metric)) {
      next.delete(row.row.metric)
    } else {
      next.add(row.row.metric)
    }
    expandedIds = next
  }
  function values(row: RenderableMetric): number[] {
    return row.row.cells.map((c) => parseFormatted(c.value))
  }

  // Sticky header cells: the box-shadow paints `--header-bg` outward to cover the grid gaps
  // (gap-x 0.75rem, gap-y 0.5rem) so scrolling rows below remain hidden. Height + leading
  // force uniform header height regardless of intrinsic font size of each cell.
  const blockHeader =
    'sticky -top-2 z-[1] mb-2 h-5 leading-5 shadow-[0_0_0_0.5rem_var(--header-bg)]'
</script>

<div class="metrics-block rounded-container bg-white-dark px-4 py-2 shadow-sm" data-block-id={id}>
  <div class="scrollbar overflow-x-auto overflow-y-visible">
  <div
    class="grid min-w-96 items-baseline gap-x-3 gap-y-2 pb-2"
    style="grid-template-columns: minmax(8rem, 1fr) 4rem 4rem 4rem 4.5rem;"
  >
    <!-- Header row: title + Avg / Min / Max headers. The rightmost (skew) column has no header
         so the right edge is reserved for the per-row skew toggle. Sticky so it stays visible
         while the block's rows scroll. The negative top/horizontal margins + padding extend
         the white background out to cover the card's own padding when sticking. -->
    {#if title}
      <h3 class={`${blockHeader} bg-white-dark text-base font-semibold text-surface-900-100`}>{title}</h3>
    {:else}
      <span class={`${blockHeader} bg-white-dark`}></span>
    {/if}
    <div class={`${blockHeader} bg-white-dark text-right font-medium`}>Avg</div>
    <div class={`${blockHeader} bg-white-dark text-right font-medium`}>Min</div>
    <div class={`${blockHeader} bg-white-dark text-right font-medium`}>Max</div>
    <div class={`${blockHeader} bg-white-dark`}></div>

    {#each entries as entry, i (i)}
      <BarChartMetric
        label={entry.label}
        metricId={entry.row.metric}
        format={entry.format}
        values={values(entry)}
        expanded={isExpanded(entry)}
        onToggle={() => toggle(entry)}
      />
    {/each}
  </div>
  </div>
</div>

<style>
  /* Component-local theme tokens for bar chart visuals. We use Skeleton's single-tone vars
     (defined under [data-theme=...] and inherited reliably) and switch on `.dark` ourselves,
     instead of relying on Skeleton's dual-tone `*-200-800` vars inside color-mix() — which
     resolved to transparent in this setup (cascade re-parsing of the dual-tone value doesn't
     reach the data-theme scope in some browsers). Children consume these via the inherited
     custom-property cascade. Kept in a <style> block so `:global(.dark)` and `:global(body.dark)`
     can both target the same vars; Tailwind's `dark:` variant alone doesn't cover the
     `body.dark` form used elsewhere in this app. */
  .metrics-block {
    --bar-low: var(--color-surface-100);
    --bar-high: var(--color-error-300);
    --skew-low: var(--color-surface-600);
    --skew-high: var(--color-error-500);
    --header-bg: white;
  }
  :global(.dark) .metrics-block,
  :global(body.dark) .metrics-block {
    --bar-low: var(--color-surface-900);
    --bar-high: var(--color-error-700);
    --skew-low: var(--color-surface-400);
    --header-bg: var(--color-dark);
  }
</style>
