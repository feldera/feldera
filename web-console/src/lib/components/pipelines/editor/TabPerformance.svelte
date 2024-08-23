<script lang="ts">
  import PipelineMemoryGraph from '$lib/components/layout/pipelines/PipelineMemoryGraph.svelte'
  import PipelineThroughputGraph from '$lib/components/layout/pipelines/PipelineThroughputGraph.svelte'
  import { humanSize } from '$lib/functions/common/string'
  import { emptyPipelineMetrics, type PipelineMetrics } from '$lib/functions/pipelineMetrics'
  import type { ExtendedPipeline } from '$lib/services/pipelineManager'
  import { format } from 'd3-format'

  const formatQty = (v: number) => format(v >= 1000 ? '.3s' : '.0f')(v)

  let {
    pipeline,
    metrics
  }: { pipeline: { current: ExtendedPipeline }; metrics: { current: PipelineMetrics } } = $props()

  const global = $derived(metrics.current.global.at(-1))
</script>

{#if global}
  <div class="flex flex-col gap-4">
    <div class="flex w-full flex-col-reverse gap-2 lg:flex-row">
      <div class="mb-auto flex flex-col">
        <span class="text-surface-600-400 w-full border-b-2 text-center">Total records</span>
        <div
          class=" grid max-w-64 grid-flow-col grid-rows-2 gap-x-4 lg:grid-flow-row lg:grid-cols-2">
          <!-- <span class="col-span-2">Records</span> -->
          <span>Ingested:</span>
          <span class="text-end">
            {formatQty(global.total_input_records)}
          </span>
          <span>Processed:</span>
          <span class="text-end">
            {formatQty(global.total_processed_records)}
          </span>
          <span>Buffered:</span>
          <span class="text-end">
            {formatQty(global.buffered_input_records)}
          </span>
        </div>
      </div>
      <div class="flex w-full flex-col md:flex-row">
        <div class="relative h-44 w-full">
          <PipelineThroughputGraph metrics={metrics.current} refetchMs={500} keepMs={60 * 1000}
          ></PipelineThroughputGraph>
        </div>
        <div class="relative h-44 w-full">
          <PipelineMemoryGraph
            metrics={metrics.current}
            refetchMs={500}
            keepMs={60 * 1000}
            {pipeline}>
          </PipelineMemoryGraph>
        </div>
      </div>
    </div>
    <div class="grid max-w-[800px] grid-cols-5">
      <span class="text-surface-600-400 border-b-2 text-center">Table</span>
      <span class="text-surface-600-400 border-b-2 text-center">Ingested records</span>
      <span class="text-surface-600-400 border-b-2 text-center">Ingested bytes</span>
      <span class="text-surface-600-400 border-b-2 text-center">Parse errors</span>
      <span class="text-surface-600-400 border-b-2 text-center">Transport errors</span>
      {#each metrics.current.input.entries() as [relation, stats]}
        <span>
          {relation}
        </span>
        <span class="mr-4 text-end">
          {formatQty(stats.total_records)}
        </span>
        <span class="mr-4 text-end">
          {humanSize(stats.total_bytes)}
        </span>
        <span class="mr-4 text-end">{formatQty(stats.num_parse_errors)} </span>
        <span class="mr-4 text-end">{formatQty(stats.num_transport_errors)} </span>
      {/each}
    </div>
    {#if metrics.current.output.size}
      <div class="grid max-w-[960px] grid-cols-6">
        <span class="text-surface-600-400 border-b-2 text-center">View</span>
        <span class="text-surface-600-400 border-b-2 text-center">Transmitted records</span>
        <span class="text-surface-600-400 border-b-2 text-center">Transmitted bytes</span>
        <span class="text-surface-600-400 border-b-2 text-center">Processed records</span>
        <span class="text-surface-600-400 border-b-2 text-center">Encode errors</span>
        <span class="text-surface-600-400 border-b-2 text-center">Transport errors</span>
        {#each metrics.current.output.entries() as [relation, stats]}
          <span>
            {relation}
          </span>
          <span class="mr-4 text-end">
            {formatQty(stats.transmitted_records)}
          </span>
          <span class="mr-4 text-end">
            {humanSize(stats.transmitted_bytes)}
          </span>
          <span class="mr-4 text-end">{formatQty(stats.total_processed_input_records)} </span>
          <span class="mr-4 text-end">{formatQty(stats.num_encode_errors)} </span>
          <span class="mr-4 text-end">{formatQty(stats.num_transport_errors)} </span>
        {/each}
      </div>
    {/if}
  </div>
{:else}
  <span class="text-surface-600-400">Pipeline is not running</span>
{/if}
