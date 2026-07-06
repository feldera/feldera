<script lang="ts">
  // K11 — binned size histogram. Each worker holds an array of batch sizes; the sizes are binned
  // into a shared histogram. Worker-averaged by default; hover left→right switches worker.
  import type { MetricGroup } from '../dispatch'
  import { asArray } from './metricData'
  import WorkerSwitchHistogram from './WorkerSwitchHistogram.svelte'

  interface Props {
    group: MetricGroup
  }
  const { group }: Props = $props()

  const BIN_COUNT = 12

  // Bin every worker's batch sizes over a shared [min, max] range so the per-worker histograms
  // are comparable when switching between them.
  const perWorker = $derived.by(() => {
    const arrays = (group.rows[0]?.values ?? []).map((v) => asArray(v))
    if (arrays.length === 0 || arrays.some((a) => a === undefined)) {
      return []
    }
    const all = arrays as number[][]
    let min = Infinity
    let max = -Infinity
    for (const a of all) {
      for (const x of a) {
        min = Math.min(min, x)
        max = Math.max(max, x)
      }
    }
    if (!Number.isFinite(min) || !Number.isFinite(max)) {
      return all.map(() => [])
    }
    const span = max - min || 1
    return all.map((sizes) => {
      const bins = new Array<number>(BIN_COUNT).fill(0)
      for (const x of sizes) {
        const idx = Math.min(BIN_COUNT - 1, Math.floor(((x - min) / span) * BIN_COUNT))
        bins[idx]!++
      }
      return bins
    })
  })
</script>

{#if perWorker.length === 0}
  <div class="text-sm text-surface-600-400">No size distribution reported.</div>
{:else}
  <div class="text-xs text-surface-600-400">count per size bin (log)</div>
  <WorkerSwitchHistogram {perWorker} categoryTitle="Size bin" unit="batches" />
{/if}
