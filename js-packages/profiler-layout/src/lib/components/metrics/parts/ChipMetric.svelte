<script lang="ts">
  // K4 — per-worker categorical / string. One chip per worker; one chip-row per string sub-field
  // (e.g. retainment_bounds → key_bounds, value_bounds).
  import { Tooltip } from 'common-ui'
  import type { MetricGroup } from '../dispatch'
  import { isMissing } from './metricData'

  interface Props {
    group: MetricGroup
  }
  const { group }: Props = $props()
</script>

<div class="flex flex-col gap-2">
  {#each group.rows as row (row.suffix)}
    <div>
      {#if group.rows.length > 1}
        <div class="mb-1 text-xs font-medium text-surface-700-300">{row.label}</div>
      {/if}
      <div class="flex flex-wrap gap-1">
        {#each row.values as v, i (i)}
          <span
            class="rounded-base bg-surface-200-800 px-2 py-0.5 text-xs text-surface-800-200"
            class:opacity-50={isMissing(v)}
          >
            {isMissing(v) ? '–' : v.toString()}
          </span>
          <Tooltip class="whitespace-nowrap" placement="top">Worker {i}</Tooltip>
        {/each}
      </div>
    </div>
  {/each}
</div>
