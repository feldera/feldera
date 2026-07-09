<script lang="ts">
  // K5 — per-worker boolean. A read-only checkbox per worker plus a "k of N true" summary.
  import { Tooltip } from 'common-ui'
  import type { MetricGroup } from '../dispatch'
  import { isMissing, numeric } from './metricData'

  interface Props {
    group: MetricGroup
  }
  const { group }: Props = $props()

  const values = $derived(group.rows[0]?.values ?? [])
  const trueCount = $derived(values.filter((v) => !isMissing(v) && numeric(v) !== 0).length)
  const total = $derived(values.filter((v) => !isMissing(v)).length)
</script>

<div class="flex flex-col gap-2">
  <div class="flex flex-wrap gap-1.5">
    {#each values as v, i (i)}
      <div>
        <input
          type="checkbox"
          class="pointer-events-none"
          checked={!isMissing(v) && numeric(v) !== 0}
          disabled
          aria-label={`Worker ${i}`}
        />
        <Tooltip class="whitespace-nowrap" placement="top">Worker {i}: {v.toString()}</Tooltip>
      </div>
    {/each}
  </div>
  <div class="text-xs tabular-nums text-surface-600-400">{trueCount} of {total} true</div>
</div>
