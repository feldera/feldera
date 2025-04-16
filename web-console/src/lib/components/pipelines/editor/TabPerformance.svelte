<script lang="ts">
  import PipelineMemoryGraph from '$lib/components/layout/pipelines/PipelineMemoryGraph.svelte'
  import PipelineThroughputGraph from '$lib/components/layout/pipelines/PipelineThroughputGraph.svelte'
  import { humanSize } from '$lib/functions/common/string'
  import { type PipelineMetrics } from '$lib/functions/pipelineMetrics'
  import type { ExtendedPipeline } from '$lib/services/pipelineManager'
  import { format } from 'd3-format'
  import Dayjs from 'dayjs'
  import { getDeploymentStatusLabel } from '$lib/functions/pipelines/status'
  import { useInterval } from '$lib/compositions/common/useInterval.svelte'
  import ClipboardCopyButton from '$lib/components/other/ClipboardCopyButton.svelte'

  const formatQty = (v: number) => format(',.0f')(v)

  let {
    pipeline,
    metrics
  }: { pipeline: { current: ExtendedPipeline }; metrics: { current: PipelineMetrics } } = $props()

  const global = $derived(metrics.current.global.at(-1))
  const now = useInterval(() => new Date(), 1000, 1000 - (Date.now() % 1000))
</script>

{#if global}
  <div class="flex h-full flex-col gap-4 overflow-y-auto overflow-x-clip scrollbar">
    <div class="flex w-full flex-col gap-4">
      <table class="mt-2 w-full max-w-[1100px] table-auto border-separate border-spacing-1 sm:mt-0">
        <thead class="align-top text-sm sm:text-base">
          <tr>
            <th class="text-start font-semibold"> Records Ingested </th>
            <th class="text-start font-semibold"> Records Processed </th>
            <th class="text-start font-semibold"> Records Buffered </th>
            <th class="text-start font-semibold"> Last status update </th>
            <th class="text-start font-semibold"> Pipeline ID </th>
          </tr>
        </thead>
        <tbody class="align-top">
          <tr>
            <td class="pt-2">
              {formatQty(global.total_input_records)}
            </td>
            <td class="pt-2">
              {formatQty(global.total_processed_records)}
            </td>
            <td class="pt-2">
              {formatQty(global.buffered_input_records)}
            </td>
            <td class="pt-2">
              {getDeploymentStatusLabel(pipeline.current.status)} for
              {((d) =>
                ((d) => (d ? ` ${d}d` : ''))(Math.floor(d.asDays())) +
                ((d) => (d ? ` ${d}h` : ''))(d.hours()) +
                ((d) => (d ? ` ${d}m` : ''))(d.minutes()) +
                ((d) => (d ? ` ${d}s` : ''))(d.seconds()))(
                Dayjs.duration(
                  now.current.valueOf() - new Date(pipeline.current.deploymentStatusSince).valueOf()
                )
              )}
              since {Dayjs(pipeline.current.deploymentStatusSince).format('MMM D, YYYY h:mm A')}
            </td>
            <td class="pt-2">
              <span class=" break-all">{pipeline.current.id}</span>
              <ClipboardCopyButton value={pipeline.current.id} class=" -mt-2 h-4 pb-0"
              ></ClipboardCopyButton>
            </td>
          </tr>
        </tbody>
      </table>
      <div class="flex w-full flex-col gap-4 md:flex-row">
        <div class="bg-white-dark relative h-52 w-full max-w-[700px] rounded">
          <PipelineThroughputGraph
            {pipeline}
            metrics={metrics.current}
            refetchMs={1000}
            keepMs={60 * 1000}
          ></PipelineThroughputGraph>
        </div>
        <div class="bg-white-dark relative h-52 w-full max-w-[700px] rounded">
          <PipelineMemoryGraph
            {pipeline}
            metrics={metrics.current}
            refetchMs={1000}
            keepMs={60 * 1000}
          ></PipelineMemoryGraph>
        </div>
      </div>
    </div>
    <div class="flex flex-wrap gap-4">
      {#if metrics.current.tables.size}
        <table class="bg-white-dark table max-w-[1000px] rounded text-base">
          <thead>
            <tr>
              <th class="font-normal text-surface-600-400">Table</th>
              <th class="!text-end font-normal text-surface-600-400">Ingested records</th>
              <th class="!text-end font-normal text-surface-600-400">Ingested bytes</th>
              <th class="!text-end font-normal text-surface-600-400">Parse errors</th>
              <th class="!text-end font-normal text-surface-600-400">Transport errors</th>
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
        <table class="bg-white-dark table max-w-[1300px] rounded text-base">
          <thead>
            <tr>
              <th class="font-normal text-surface-600-400">View</th>
              <th class="!text-end font-normal text-surface-600-400">Transmitted records</th>
              <th class="!text-end font-normal text-surface-600-400">Transmitted bytes</th>
              <th class="!text-end font-normal text-surface-600-400">Buffered records</th>
              <th class="!text-end font-normal text-surface-600-400">Buffered batches</th>
              <th class="!text-end font-normal text-surface-600-400">Encode errors</th>
              <th class="!text-end font-normal text-surface-600-400">Transport errors</th>
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
                <td class="text-end">{formatQty(stats.buffered_records)} </td>
                <td class="text-end">{formatQty(stats.buffered_batches)} </td>
                <td class="text-end">{formatQty(stats.num_encode_errors)} </td>
                <td class="text-end">{formatQty(stats.num_transport_errors)} </td>
              </tr>
            {/each}
          </tbody>
        </table>
      {/if}
    </div>
  </div>
{:else}
  <div>Pipeline is not running</div><br />
  Pipeline ID: {pipeline.current.id}
  <ClipboardCopyButton value={pipeline.current.id} class=" -mt-2 h-4 pb-0"></ClipboardCopyButton>
{/if}
