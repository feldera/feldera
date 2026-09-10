<script lang="ts">
  import type { GlobalMetricEntry } from '../../../functions/globalMetrics'
  import { isCollapsed, toggleCollapsed } from '../collapsedBlocks.svelte'
  import BlockTitle from '../parts/BlockTitle.svelte'

  interface Props {
    id: string
    title?: string
    /** Rows to print, each already carrying a formatted `PropertyValue`. */
    entries: GlobalMetricEntry[]
  }
  const { id, title, entries }: Props = $props()

  const collapsed = $derived(isCollapsed(id))
</script>

<!-- A plain text tile: one "label … value" row per metric. Unlike the distribution block it
     has no per-worker bars — these are single pipeline-wide figures — so the value is rendered
     straight from `PropertyValue.toString()`, reusing the same unit-aware formatting. -->
<div class="rounded-base bg-white-dark px-4 py-1.5 shadow-sm" data-block-id={id}>
  {#if title}
    <BlockTitle
      class={collapsed ? '' : 'mb-1.5'}
      {title}
      {collapsed}
      onToggle={() => toggleCollapsed(id)}
    />
  {/if}
  {#if !collapsed}
    <dl class="grid grid-cols-[1fr_auto] items-baseline gap-x-6 gap-y-1.5">
      {#each entries as entry (entry.key)}
        <dt class="truncate text-sm text-surface-700-300">{entry.label}</dt>
        <dd class="text-right text-sm font-medium tabular-nums text-surface-900-100">
          {entry.value.toString()}
        </dd>
      {/each}
    </dl>
  {/if}
</div>
