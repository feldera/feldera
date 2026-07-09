<script lang="ts">
  // K6 — per-(worker × level) categorical state. Grid of state chips: one row per spine level
  // (slot), one cell per worker, colored by status (none / requested / in progress).
  import { Tooltip } from 'common-ui'
  import type { MetricGroup, MetricSubRow } from '../dispatch'
  import { isMissing, workerCount } from './metricData'

  interface Props {
    group: MetricGroup
  }
  const { group }: Props = $props()

  // Sub-rows are labeled per slot, e.g. suffix '.slot:0'. Parse the level and sort ascending.
  const levels = $derived.by(() => {
    const out: Array<{ slot: number; row: MetricSubRow }> = []
    for (const row of group.rows) {
      const m = row.suffix.match(/slot:(\d+)/)
      if (m) {
        out.push({ slot: Number(m[1]), row })
      }
    }
    out.sort((a, b) => a.slot - b.slot)
    return out
  })

  const n = $derived(workerCount(group))
  const workers = $derived(Array.from({ length: n }, (_, i) => i))

  function stateColor(s: string): string {
    const t = s.toLowerCase()
    if (t.includes('progress')) {
      return 'var(--state-progress)'
    }
    if (t.includes('request')) {
      return 'var(--state-requested)'
    }
    return 'var(--state-none)'
  }
</script>

{#if levels.length === 0}
  <div class="text-sm text-surface-600-400">No compaction state reported.</div>
{:else}
  <div class="flex flex-col gap-1">
    <!-- Header: "Worker" outside the grid on the left; worker numbers over every 4th column,
         using the same grid template as the chip rows so they stay aligned. -->
    <div class="flex items-center gap-2">
      <span class="w-14 shrink-0 text-xs text-surface-600-400">Worker</span>
      <div
        class="grid flex-1 gap-0.5"
        style:grid-template-columns="repeat({n}, minmax(0, 1fr))"
        style:max-width="{n * 14}px"
      >
        {#each workers as i (i)}
          <span class="text-center text-[10px] tabular-nums text-surface-500">
            {i % 4 === 0 ? i : ''}
          </span>
        {/each}
      </div>
    </div>
    {#each levels as { slot, row } (slot)}
      <div class="flex items-center gap-2">
        <span class="w-14 shrink-0 text-sm tabular-nums text-surface-700-300">Slot {slot}</span>
        <!-- One column per worker: columns share the row width, so a large worker count
             compresses the chips horizontally instead of wrapping or scrolling. A max width
             keeps chips square-ish when workers are few. -->
        <div
          class="grid flex-1 gap-0.5"
          style:grid-template-columns="repeat({n}, minmax(0, 1fr))"
          style:max-width="{n * 14}px"
        >
          {#each workers as i (i)}
            {@const v = row.values[i]}
            <div
              class="h-3 w-full rounded-[2px]"
              style:background-color={isMissing(v) ? 'transparent' : stateColor(v.toString())}
            ></div>
            <Tooltip class="whitespace-nowrap" placement="top">
              Worker {i}: {isMissing(v) ? '–' : v.toString()}
            </Tooltip>
          {/each}
        </div>
      </div>
    {/each}
  </div>
{/if}
