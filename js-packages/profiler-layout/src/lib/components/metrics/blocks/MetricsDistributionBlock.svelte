<script lang="ts">
  import { isCollapsed, toggleCollapsed } from '../collapsedBlocks.svelte'
  import type { RenderableMetric } from '../dispatch'
  import BarChartMetric from '../parts/BarChartMetric.svelte'
  import BlockTitle from '../parts/BlockTitle.svelte'

  interface Props {
    id: string
    title?: string
    entries: RenderableMetric[]
  }
  const { id, title, entries }: Props = $props()

  // The block holding the current metric stays open whatever the user chose for its category:
  // that metric's value is what the panel leads with. The choice itself is kept, so the block
  // collapses again once the current metric moves to another category.
  const holdsCurrent = $derived(entries.some((entry) => entry.row.isCurrentMetric))
  // A block with no title has no toggle to click, so it never collapses.
  const collapsed = $derived(title !== undefined && isCollapsed(id) && !holdsCurrent)

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

  // Sticky header cells: the box-shadow paints `--header-bg` outward over the card's 0.375rem
  // vertical padding and half of each 0.75rem column gap, so scrolling rows stay hidden behind
  // the header and the paint stops at the card's edge. Height + leading force uniform header
  // height regardless of intrinsic font size of each cell.
  const blockHeader =
    'sticky -top-1.5 z-[1] mb-1.5 h-5 leading-5 shadow-[0_0_0_0.375rem_var(--header-bg)]'
</script>

{#snippet blockTitle(className: string)}
  <BlockTitle
    class={className}
    title={title ?? ''}
    {collapsed}
    pinned={holdsCurrent ? 'Holds the current metric, so it cannot be collapsed' : undefined}
    onToggle={() => toggleCollapsed(id)}
  />
{/snippet}

<div class="metrics-block rounded-base bg-white-dark px-4 py-1.5 shadow-sm" data-block-id={id}>
  {#if collapsed}
    {@render blockTitle('h-5 leading-5')}
  {:else}
    <div class="scrollbar overflow-x-auto overflow-y-visible">
    <div
      class="grid min-w-120 items-baseline gap-x-3 gap-y-0"
      style="grid-template-columns: minmax(8rem, 1fr) 4rem 4rem 4rem 4.5rem 4.5rem;"
    >
      <!-- Header row: title + Avg / Min / Max / Total headers. Total is blank for metrics that
           cannot be added. The rightmost (skew) column has no header so the right edge is
           reserved for the per-row skew toggle. Sticky so it stays visible while the block's rows
           scroll; its shadow paints over the card's padding to hide the rows passing behind. -->
      {#if title}
        {@render blockTitle(`${blockHeader} bg-white-dark`)}
      {:else}
        <span class={`${blockHeader} bg-white-dark`}></span>
      {/if}
      <div class={`${blockHeader} bg-white-dark text-right font-medium`}>Avg</div>
      <div class={`${blockHeader} bg-white-dark text-right font-medium`}>Min</div>
      <div class={`${blockHeader} bg-white-dark text-right font-medium`}>Max</div>
      <div class={`${blockHeader} bg-white-dark text-right font-medium`}>Total</div>
      <div class={`${blockHeader} bg-white-dark`}></div>

      {#each entries as entry, i (i)}
        <BarChartMetric
          label={entry.label}
          metricId={entry.row.metric}
          cells={entry.row.cells}
          total={entry.row.total}
          current={entry.row.isCurrentMetric}
          expanded={isExpanded(entry)}
          onToggle={() => toggle(entry)}
        />
      {/each}
    </div>
    </div>
  {/if}
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
    --current-bg: var(--color-tertiary-50);
  }
  :global(.dark) .metrics-block,
  :global(body.dark) .metrics-block {
    --bar-low: var(--color-surface-900);
    --bar-high: var(--color-error-700);
    --skew-low: var(--color-surface-400);
    --header-bg: var(--color-dark);
    --current-bg: var(--color-tertiary-950);
  }
</style>
