<script lang="ts">
  // K8 — cache counts. A per-worker grid with avg_latency as the headline column, then count,
  // then bytes. (avg_latency = elapsed/count already summarizes the elapsed total.)
  import type { PropertyValue } from 'profiler-lib'
  import type { MetricGroup } from '../dispatch'
  import { rowEndingWith, workerCount } from './metricData'

  interface Props {
    group: MetricGroup
  }
  const { group }: Props = $props()

  const latency = $derived(rowEndingWith(group, '.avg_latency'))
  const count = $derived(rowEndingWith(group, '.count'))
  const bytes = $derived(rowEndingWith(group, '.bytes'))
  const n = $derived(workerCount(group))
  const workers = $derived(Array.from({ length: n }, (_, i) => i))

  const cell = (v: PropertyValue | undefined) => v?.toString() ?? '–'
</script>

<div
  class="grid gap-x-3 gap-y-0.5 text-sm tabular-nums"
  style="grid-template-columns: 3rem 1fr 1fr 1fr;"
>
  <div></div>
  <div class="text-right text-xs font-medium text-surface-600-400">avg latency</div>
  <div class="text-right text-xs font-medium text-surface-600-400">count</div>
  <div class="text-right text-xs font-medium text-surface-600-400">bytes</div>
  {#each workers as i (i)}
    <div class="text-xs text-surface-600-400">W{i}</div>
    <div class="text-right font-medium text-surface-900-100">{cell(latency?.values[i])}</div>
    <div class="text-right text-surface-700-300">{cell(count?.values[i])}</div>
    <div class="text-right text-surface-700-300">{cell(bytes?.values[i])}</div>
  {/each}
</div>
