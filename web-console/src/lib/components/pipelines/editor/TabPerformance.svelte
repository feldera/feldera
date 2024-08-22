<script lang="ts">
  import PipelineMemoryGraph from '$lib/components/layout/pipelines/PipelineMemoryGraph.svelte'
  import PipelineThroughputGraph from '$lib/components/layout/pipelines/PipelineThroughputGraph.svelte'
  import { useAggregatePipelineStats } from '$lib/compositions/useAggregatePipelineStats.svelte'
  import { humanSize } from '$lib/functions/common/string'
  import { emptyPipelineMetrics, type PipelineMetrics } from '$lib/functions/pipelineMetrics'
  import type { ExtendedPipeline } from '$lib/services/pipelineManager'

  let { pipeline }: { pipeline: { current: ExtendedPipeline } } = $props()

  let pipelineName = $derived(pipeline.current.name)

  let pipelineStats = $state({ metrics: emptyPipelineMetrics } as { metrics: PipelineMetrics })

  let pipelineStatus = $derived(pipeline.current.status)
  $effect(() => {
    if (pipelineStatus !== 'Running' && pipelineStatus !== 'Paused') {
      pipelineStats = { metrics: emptyPipelineMetrics }
      return
    }
    pipelineStats = useAggregatePipelineStats(pipelineName, 1000, 62000)
  })

  const global = $derived(pipelineStats.metrics.global.at(-1))
</script>

{#if pipelineStats.metrics.global.at(-1) && global}
  <div class="flex flex-col gap-4">
    <div class="flex w-full flex-col-reverse lg:flex-row">
      <div class="mb-auto">
        <span class="text-lg">Total records</span>
        <div
          class=" grid max-w-64 grid-flow-col grid-rows-2 gap-x-4 lg:grid-flow-row lg:grid-cols-2">
          <!-- <span class="col-span-2">Records</span> -->
          <span>Input:</span><span> {global.total_input_records} </span>
          <span>Buffered:</span><span> {global.buffered_input_records} </span>
          <span>Processed:</span><span> {global.total_processed_records} </span>
        </div>
      </div>
      <div class="flex w-full flex-col md:flex-row">
        <div class="relative h-[200px] w-full">
          <PipelineThroughputGraph metrics={pipelineStats.metrics}></PipelineThroughputGraph>
        </div>
        <div class="relative h-[200px] w-full">
          <PipelineMemoryGraph metrics={pipelineStats.metrics}></PipelineMemoryGraph>
        </div>
      </div>
    </div>
    <div class="grid max-w-[800px] grid-cols-5">
      <span class="text-surface-600-400">Table</span>
      <span class="text-surface-600-400">Records</span>
      <span class="text-surface-600-400">Bytes</span>
      <span class="text-surface-600-400">Parse errors</span>
      <span class="text-surface-600-400">Transport errors</span>
      {#each pipelineStats.metrics.input.entries() as [relation, metrics]}
        <span>
          {relation}
        </span>
        <span>
          {metrics.total_records}
        </span>
        <span>
          {metrics.total_bytes}
        </span>
        <span>{metrics.num_parse_errors} </span>
        <span>{metrics.num_transport_errors} </span>
      {/each}
    </div>
    {#if pipelineStats.metrics.output.size}
      <div class="grid max-w-[960px] grid-cols-6">
        <span class="text-surface-600-400">View</span>
        <span class="text-surface-600-400">Transmitted rec-s</span>
        <span class="text-surface-600-400">Transmitted bytes</span>
        <span class="text-surface-600-400">Processed rec-s</span>
        <span class="text-surface-600-400">Encode errors</span>
        <span class="text-surface-600-400">Transport errors</span>
        {#each pipelineStats.metrics.output.entries() as [relation, metrics]}
          <span>
            {relation}
          </span>
          <span>
            {metrics.transmitted_records}
          </span>
          <span>
            {metrics.transmitted_bytes}
          </span>
          <span>{metrics.total_processed_input_records} </span>
          <span>{metrics.num_encode_errors} </span>
          <span>{metrics.num_transport_errors} </span>
        {/each}
      </div>
    {/if}
  </div>
{:else}
  <span class="text-surface-600-400">Pipeline is not running</span>
{/if}
