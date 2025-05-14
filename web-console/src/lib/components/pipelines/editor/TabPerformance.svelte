<script lang="ts">
  import PipelineMemoryGraph from '$lib/components/layout/pipelines/PipelineMemoryGraph.svelte'
  import PipelineThroughputGraph from '$lib/components/layout/pipelines/PipelineThroughputGraph.svelte'
  import { humanSize } from '$lib/functions/common/string'
  import { type PipelineMetrics } from '$lib/functions/pipelineMetrics'
  import type { ExtendedPipeline } from '$lib/services/pipelineManager'
  import { format } from 'd3-format'
  import Dayjs from 'dayjs'
  import { getDeploymentStatusLabel, isMetricsAvailable } from '$lib/functions/pipelines/status'
  import { useInterval } from '$lib/compositions/common/useInterval.svelte'
  import ClipboardCopyButton from '$lib/components/other/ClipboardCopyButton.svelte'
  import { Segment } from '@skeletonlabs/skeleton-svelte'
  import { useIsScreenLg } from '$lib/compositions/layout/useIsMobile.svelte'

  const formatQty = (v: number) => format(',.0f')(v)

  let {
    pipeline,
    metrics
  }: { pipeline: { current: ExtendedPipeline }; metrics: { current: PipelineMetrics } } = $props()

  const global = $derived(metrics.current.global.at(-1))
  const now = useInterval(() => new Date(), 1000, 1000 - (Date.now() % 1000))

  const formatElapsedTime = (timestamp: Date) =>
    ((d) =>
      ((d) => (d ? ` ${d}d` : ''))(Math.floor(d.asDays())) +
      ((d) => (d ? ` ${d}h` : ''))(d.hours()) +
      ((d) => (d ? ` ${d}m` : ''))(d.minutes()) +
      ((d) => (d ? ` ${d}s` : ''))(d.seconds()))(
      Dayjs.duration(now.current.valueOf() - timestamp.valueOf())
    )
  let statusTab: 'age' | 'updated' | 'id' = $state('age')
  let isLg = useIsScreenLg()
</script>

{#if isMetricsAvailable(pipeline.current.status) === 'no'}
  <div>Pipeline is not running</div>
  <br />
  Pipeline ID: {pipeline.current.id}
  <ClipboardCopyButton value={pipeline.current.id} class=" -mt-2 h-4 pb-0"></ClipboardCopyButton>
{:else if !global}
  <div>Pipeline is running, but has not reported usage telemetry yet</div>
  <br />
  Pipeline ID: {pipeline.current.id}
  <ClipboardCopyButton value={pipeline.current.id} class=" -mt-2 h-4 pb-0"></ClipboardCopyButton>
{:else}<div class="flex h-full flex-col gap-2 overflow-y-auto overflow-x-clip scrollbar">
    <div class="flex w-full flex-col gap-4">
      <div class="flex max-w-[1600px] flex-wrap gap-4 pt-2">
        <div class="flex flex-col">
          <div class="text-nowrap text-start font-semibold">Records Ingested</div>
          <div class="pt-2">
            {formatQty(global.total_input_records)}
          </div>
        </div>
        <div class="flex flex-col">
          <div class="text-nowrap text-start font-semibold">
            <span class="hidden sm:inline">Records</span> Processed
          </div>
          <div class="pt-2">
            {formatQty(global.total_processed_records)}
          </div>
        </div>
        <div class="flex flex-col">
          <div class="text-nowrap text-start font-semibold">
            <span class="hidden sm:inline">Records</span> Buffered
          </div>
          <div class="pt-2">
            {formatQty(global.buffered_input_records)}
          </div>
        </div>
        {#snippet age()}
          <div class="w-96 pt-2">
            Active for
            {formatElapsedTime(new Date(global.start_time * 1000))}
            since {Dayjs(pipeline.current.deploymentStatusSince).format('MMM D, YYYY h:mm A')}
          </div>
        {/snippet}
        {#snippet updated()}
          <div class="w-96 pt-2">
            {getDeploymentStatusLabel(pipeline.current.status)} for
            {formatElapsedTime(new Date(pipeline.current.deploymentStatusSince))}
            since {Dayjs(pipeline.current.deploymentStatusSince).format('MMM D, YYYY h:mm A')}
          </div>
        {/snippet}
        {#snippet id()}
          <div class="w-[340px] pt-2">
            <span class=" break-all">{pipeline.current.id}</span>
            <ClipboardCopyButton value={pipeline.current.id} class=" -mt-2 h-4 pb-0"
            ></ClipboardCopyButton>
          </div>
        {/snippet}
        {#if isLg.current}
          <div class="flex flex-col">
            <div class="text-start font-semibold">Deployment age</div>
            {@render age()}
          </div>
          <div class="flex flex-col">
            <div class="text-start font-semibold">Last status update</div>
            {@render updated()}
          </div>
          <div class="flex flex-col">
            <div class="text-start font-semibold">Pipeline ID</div>
            {@render id()}
          </div>
        {:else}
          <div>
            <Segment
              bind:value={statusTab}
              background="preset-filled-surface-50-950 w-fit flex-none -mt-3"
              indicatorBg="bg-white-dark shadow"
              indicatorText=""
              border="p-1"
              rounded="rounded"
            >
              <Segment.Item value="age" base="btn cursor-pointer z-[1] px-5 py-4 h-6">
                <div class="text-start font-semibold">Age</div>
              </Segment.Item>
              <Segment.Item value="updated" base="btn cursor-pointer z-[1] px-5 py-4 h-6">
                <div class="text-start font-semibold">Last status update</div>
              </Segment.Item>
              <Segment.Item value="id" base="btn cursor-pointer z-[1] px-5 py-4 h-6">
                <div class="text-start font-semibold">Pipeline ID</div>
              </Segment.Item>
            </Segment>
            {#if statusTab === 'age'}
              {@render age()}
            {:else if statusTab === 'updated'}
              {@render updated()}
            {:else}
              {@render id()}
            {/if}
          </div>
        {/if}
      </div>
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
        <table class="bg-white-dark table h-min max-w-[1000px] rounded text-base">
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
        <table class="bg-white-dark table h-min max-w-[1300px] rounded text-base">
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
{/if}
