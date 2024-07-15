<script lang="ts">
  import PipelineMemoryGraph from '$lib/components/layout/pipelines/PipelineMemoryGraph.svelte'
  import { useAggregatePipelineStats } from '$lib/compositions/useAggregatePipelineStats.svelte'
  import { humanSize } from '$lib/functions/common/string'
  import { emptyPipelineMetrics, type PipelineMetrics } from '$lib/functions/pipelineMetrics'

  let { pipelineName: _pipelineName }: { pipelineName: string } = $props()

  let pipelineName = $state(_pipelineName)
  $effect(() => {
    pipelineName = _pipelineName
  })

  let pipelineStats = $state({ metrics: emptyPipelineMetrics } as { metrics: PipelineMetrics })

  $effect(() => {
    pipelineStats = useAggregatePipelineStats(pipelineName, 1000, 5000)
  })

  const global = $derived(pipelineStats.metrics.global.at(-1))
</script>

<!-- <div>global {global}</div> -->
<!-- <div>{JSON.stringify(pipelineStats.metrics.global)}</div> -->
<div class="relative h-full w-full">
  <div class="absolute h-full w-full">
    {#if pipelineStats.metrics.global.at(-1) && global}
      <div class="flex gap-4">
        <span>Pipeline Memory: {humanSize(global.rss_bytes)} </span>
        <span>Input records: {global.total_input_records} </span>
        <span>Buffered records: {global.buffered_input_records} </span>
        <span>Processed records: {global.total_processed_records} </span>
      </div>
      <div>
        {#each pipelineStats.metrics.input.entries() as [relation, metrics]}
          <div class="flex gap-4">
            <span>
              Table {relation}: Total records: {metrics.total_records}, bytes: {metrics.total_bytes}
            </span>
            <span>Parse errors: {metrics.num_parse_errors} </span>
            <span>Transport errors: {metrics.num_transport_errors} </span>
          </div>
        {/each}
      </div>
      <div>
        {#each pipelineStats.metrics.output.entries() as [relation, metrics]}
          <div class="flex gap-4">
            <span>
              View {relation}: Transmitted records: {metrics.transmitted_records}, bytes: {metrics.transmitted_bytes}
            </span>
            <span>Total processed records: {metrics.total_processed_input_records} </span>
            <span>Encode errors: {metrics.num_encode_errors} </span>
            <span>Transport errors: {metrics.num_transport_errors} </span>
          </div>
        {/each}
      </div>
    {:else}
      <span class="text-surface-600-400">Pipeline is not running</span>
    {/if}
    <!-- <PipelineMemoryGraph metrics={pipelineStats.metrics}></PipelineMemoryGraph> -->
  </div>
</div>
