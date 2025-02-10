<script lang="ts">
  import PipelineMemoryGraph from '$lib/components/layout/pipelines/PipelineMemoryGraph.svelte'
  import PipelineThroughputGraph from '$lib/components/layout/pipelines/PipelineThroughputGraph.svelte'
  import { humanSize } from '$lib/functions/common/string'
  import { type PipelineMetrics } from '$lib/functions/pipelineMetrics'
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
  <div class="flex h-full flex-col gap-4 overflow-y-auto overflow-x-clip scrollbar">
    <div class="flex w-full flex-col-reverse gap-4 lg:flex-row">
      <div class="bg-white-dark mb-auto mr-auto flex flex-col rounded px-4 py-2">
        <div class="-mx-4 border-b-2 pb-2 text-center">Total records</div>
        <div
          class="grid max-w-64 grid-flow-col grid-rows-2 gap-x-4 gap-y-2 pt-2 lg:grid-flow-row lg:grid-cols-2"
        >
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
      <div class="flex w-full flex-col gap-4 md:flex-row">
        <div class="bg-white-dark relative h-52 w-full rounded">
          <PipelineThroughputGraph
            {pipeline}
            metrics={metrics.current}
            refetchMs={1000}
            keepMs={60 * 1000}
          ></PipelineThroughputGraph>
        </div>
        <div class="bg-white-dark relative h-52 w-full rounded">
          <PipelineMemoryGraph
            {pipeline}
            metrics={metrics.current}
            refetchMs={1000}
            keepMs={60 * 1000}
          ></PipelineMemoryGraph>
        </div>
      </div>
    </div>
    {#if metrics.current.tables.size}
      <table class="bg-white-dark table max-w-[1000px] rounded text-base">
        <thead>
          <tr>
            <th class="text-center font-normal text-surface-600-400">Table</th>
            <th class="text-center font-normal text-surface-600-400">Ingested records</th>
            <th class="text-center font-normal text-surface-600-400">Ingested bytes</th>
            <th class="text-center font-normal text-surface-600-400">Parse errors</th>
            <th class="text-center font-normal text-surface-600-400">Transport errors</th>
          </tr>
        </thead>
        <tbody>
          {#each metrics.current.tables.entries() as [relation, stats]}
            <tr>
              <td>
                {relation}
              </td>
              <td class="text-end">
                {formatQty(stats.total_records)}
              </td>
              <td class="text-end">
                {humanSize(stats.total_bytes)}
              </td>
              <td class="text-end">{formatQty(stats.num_parse_errors)} </td>
              <td class="text-end">{formatQty(stats.num_transport_errors)} </td>
            </tr>
          {/each}
        </tbody>
      </table>
    {/if}
    {#if metrics.current.views.size}
      <table class="bg-white-dark table max-w-[1200px] rounded text-base">
        <thead>
          <tr>
            <th class="text-center font-normal text-surface-600-400">View</th>
            <th class="text-center font-normal text-surface-600-400">Transmitted records</th>
            <th class="text-center font-normal text-surface-600-400">Transmitted bytes</th>
            <th class="text-center font-normal text-surface-600-400">Processed records</th>
            <th class="text-center font-normal text-surface-600-400">Encode errors</th>
            <th class="text-center font-normal text-surface-600-400">Transport errors</th>
          </tr>
        </thead>
        <tbody>
          {#each metrics.current.views.entries() as [relation, stats]}
            <tr>
              <td>
                {relation}
              </td>
              <td class="text-end">
                {formatQty(stats.transmitted_records)}
              </td>
              <td class="text-end">
                {humanSize(stats.transmitted_bytes)}
              </td>
              <td class="text-end">{formatQty(stats.total_processed_input_records)} </td>
              <td class="text-end">{formatQty(stats.num_encode_errors)} </td>
              <td class="text-end">{formatQty(stats.num_transport_errors)} </td>
            </tr>
          {/each}
        </tbody>
      </table>
    {/if}
  </div>
{:else}
  <span class="flex text-surface-600-400">Pipeline is not running</span>
{/if}
