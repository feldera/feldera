<script lang="ts">
  // K10 — worker-fanout matrix. Each worker (row = source) reports an array indexed by
  // destination worker; together an N×N matrix. Rendered as a vertically-squished heatmap so
  // cross-worker skew is visible at a glance.
  import { Tooltip } from 'common-ui'
  import { lerpThemeColor } from '../colors'
  import type { MetricGroup } from '../dispatch'
  import { asArray } from './metricData'

  interface Props {
    group: MetricGroup
  }
  const { group }: Props = $props()

  const matrix = $derived.by(() => {
    const rows = (group.rows[0]?.values ?? []).map((v) => asArray(v))
    if (rows.length === 0 || rows.some((r) => r === undefined)) {
      return null
    }
    const grid = rows as number[][]
    const peak = grid.reduce((m, r) => r.reduce((mm, v) => Math.max(mm, v), m), 0)
    return { grid, peak }
  })
</script>

{#if !matrix}
  <div class="text-sm text-surface-600-400">No key distribution reported.</div>
{:else}
  <div class="flex flex-col gap-0.5">
    {#each matrix.grid as row, src (src)}
      <div class="flex items-center gap-1">
        <span class="w-8 shrink-0 text-xs tabular-nums text-surface-600-400">W{src}</span>
        <div class="flex flex-1 gap-px">
          {#each row as v, dst (dst)}
            <div
              class="h-3 flex-1 rounded-[1px]"
              style:background-color={lerpThemeColor(
                matrix.peak > 0 ? v / matrix.peak : 0,
                '--bar-low',
                '--bar-high'
              )}
            ></div>
            <Tooltip class="whitespace-nowrap" placement="top">
              W{src} → W{dst}: {v.toLocaleString('en-US')}
            </Tooltip>
          {/each}
        </div>
      </div>
    {/each}
  </div>
{/if}
