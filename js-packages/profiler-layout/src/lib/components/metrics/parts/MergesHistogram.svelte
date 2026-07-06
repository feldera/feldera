<script lang="ts">
  // K9 — merge progress per (worker × level). Histogram over spine levels (x = slot, y = steps);
  // the worker-averaged shape shows by default, hovering left→right switches to one worker.
  import type { MetricGroup } from '../dispatch'
  import { numeric, workerCount } from './metricData'
  import WorkerSwitchHistogram from './WorkerSwitchHistogram.svelte'

  interface Props {
    group: MetricGroup
  }
  const { group }: Props = $props()

  // perWorker[worker][slot] = steps at that level. Built from '.slot:<n>.steps' sub-rows.
  const perWorker = $derived.by(() => {
    const n = workerCount(group)
    let maxSlot = -1
    const stepRows: Array<{ slot: number; values: (number | undefined)[] }> = []
    for (const row of group.rows) {
      const m = row.suffix.match(/slot:(\d+)\.steps$/)
      if (m) {
        const slot = Number(m[1])
        maxSlot = Math.max(maxSlot, slot)
        stepRows.push({ slot, values: row.values.map((v) => numeric(v)) })
      }
    }
    if (maxSlot < 0) {
      return []
    }
    const out: number[][] = Array.from({ length: n }, () => new Array<number>(maxSlot + 1).fill(0))
    for (const { slot, values } of stepRows) {
      for (let w = 0; w < n; w++) {
        out[w]![slot] = values[w] ?? 0
      }
    }
    return out
  })
</script>

{#if perWorker.length === 0}
  <div class="text-sm text-surface-600-400">No completed merges reported.</div>
{:else}
  <div class="text-sm text-surface-600-400">steps per level (log)</div>
  <WorkerSwitchHistogram {perWorker} categoryTitle="Spine level" unit="steps" />
{/if}
