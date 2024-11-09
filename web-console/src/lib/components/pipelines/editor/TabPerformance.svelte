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
  <div class="flex h-full flex-col gap-4 overflow-y-auto p-2 scrollbar">
    <div class="flex w-full flex-col-reverse gap-2 lg:flex-row">
      <div class="mr-auto">
        <div class="mb-auto flex flex-col">
          <div class="border-b-2 text-center text-surface-600-400">Total records</div>
          <div
            class=" grid max-w-64 grid-flow-col grid-rows-2 gap-x-4 lg:grid-flow-row lg:grid-cols-2"
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
      </div>
      <div class="flex w-full flex-col md:flex-row">
        <div class="relative h-44 w-full">
          <PipelineThroughputGraph
            {pipeline}
            metrics={metrics.current}
            refetchMs={1000}
            keepMs={60 * 1000}
          ></PipelineThroughputGraph>
        </div>
        <div class="relative h-44 w-full">
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
      <table class="max-w-[1000px]">
        <thead class="border-b-2">
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
      <table class="max-w-[1200px]">
        <thead class="border-b-2">
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
  <span class="flex p-2 text-surface-600-400">Pipeline is not running</span>
{/if}
