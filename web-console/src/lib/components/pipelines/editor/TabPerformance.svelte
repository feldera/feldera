<script lang="ts">
  import PipelineMemoryGraph from '$lib/components/layout/pipelines/PipelineMemoryGraph.svelte'
  import PipelineThroughputGraph from '$lib/components/layout/pipelines/PipelineThroughputGraph.svelte'
  import { humanSize } from '$lib/functions/common/string'
  import { type PipelineMetrics } from '$lib/functions/pipelineMetrics'
  import type { ExtendedPipeline } from '$lib/services/pipelineManager'
  import { format } from 'd3-format'
  import Dayjs from 'dayjs'
  import { getDeploymentStatusLabel, isMetricsAvailable } from '$lib/functions/pipelines/status'
  import ClipboardCopyButton from '$lib/components/other/ClipboardCopyButton.svelte'
  import { Segment } from '@skeletonlabs/skeleton-svelte'
  import { useIsScreenXl } from '$lib/compositions/layout/useIsMobile.svelte'
  import PipelineStorageGraph from '$lib/components/layout/pipelines/PipelineStorageGraph.svelte'
  import Tooltip from '$lib/components/common/Tooltip.svelte'
  import { useElapsedTime } from '$lib/functions/format'
  import {
    usePipelineManager,
    type PipelineManagerApi
  } from '$lib/compositions/usePipelineManager.svelte'
  import {
    CustomJSONParserTransformStream,
    parseCancellable,
    pushAsCircularBuffer
  } from '$lib/functions/pipelines/changeStream'
  import type { TimeSeriesEntry } from '$lib/types/pipelineManager'

  const formatQty = (v: number) => format(',.0f')(v)

  let {
    pipeline,
    metrics
  }: { pipeline: { current: ExtendedPipeline }; metrics: { current: PipelineMetrics } } = $props()

  const global = $derived(metrics.current.global)
  const { formatElapsedTime } = useElapsedTime()

  let timeSeries: TimeSeriesEntry[] = $state([])

  let statusTab: 'age' | 'updated' = $state('age')
  let isXl = useIsScreenXl()
  let api = usePipelineManager()

  let cancelStream: (() => void) | undefined = undefined

  const endMetricsStream = (id?: string) => {
    cancelStream?.()
    cancelStream = undefined
    timeSeries = []
  }
  const startMetricsStream = async (api: PipelineManagerApi, targetPipelineName: string) => {
    const result = await api.pipelineTimeSeriesStream(pipelineName)
    if (result instanceof Error) {
      cancelStream = undefined
      return undefined
    }
    const { cancel } = parseCancellable(
      result,
      {
        pushChanges: (rows: TimeSeriesEntry[]) => {
          pushAsCircularBuffer(
            () => timeSeries,
            63,
            (v: TimeSeriesEntry) => v
          )(rows)
        },
        onBytesSkipped: (skippedBytes) => {},
        onParseEnded: () => {
          if (metricsAvailable && cancelStream) {
            endMetricsStream()
            if (pipelineName !== targetPipelineName) {
              return
            }
            startMetricsStream(api, targetPipelineName)
          }
        },
        onNetworkError: () => {
          if (metricsAvailable && cancelStream) {
            endMetricsStream()
            if (pipelineName !== targetPipelineName) {
              return
            }
            startMetricsStream(api, targetPipelineName)
          }
        }
      },
      new CustomJSONParserTransformStream<TimeSeriesEntry>({
        paths: ['$'],
        separator: ''
      }),
      {
        bufferSize: 8 * 1024 * 1024
      }
    )
    cancelStream = cancel
  }

  let pipelineName = $derived(pipeline.current.name)
  let metricsAvailable = $derived(isMetricsAvailable(pipeline.current.status) === 'yes')

  $effect(() => {
    pipelineName
    if (!metricsAvailable) {
      endMetricsStream()
      return
    }
    $effect.root(() => {
      if (cancelStream) {
        endMetricsStream()
      }
      setTimeout(() => startMetricsStream(api, pipelineName), 100)
    })
  })
</script>

{#snippet pipelineId()}
  <ClipboardCopyButton
    value={pipeline.current.id}
    class="ml-auto h-8 w-auto border-[1px] border-surface-200-800"
  >
    <span class="text-base font-normal text-surface-950-50"
      ><span class="hidden sm:inline">Copy pipeline ID</span><span class="inline sm:hidden">
        Pipeline ID</span
      ></span
    >
  </ClipboardCopyButton>
  <Tooltip
    placement="top"
    class="z-10 text-nowrap rounded bg-white text-base text-surface-950-50 dark:bg-black"
  >
    {pipeline.current.id}
  </Tooltip>
{/snippet}

{#if isMetricsAvailable(pipeline.current.status) === 'no'}
  <div class="flex justify-between pt-2 sm:pt-0">
    <div>Pipeline is not running</div>
    {@render pipelineId()}
  </div>
{:else if pipeline.current.status === 'Unavailable'}
  <div class="flex justify-between">
    <div>
      Pipeline is unavailable for {formatElapsedTime(
        new Date(pipeline.current.deploymentStatusSince)
      )} since {Dayjs(pipeline.current.deploymentStatusSince).format('MMM D, YYYY h:mm A')}. You can
      attempt to suspend or shut it down.
    </div>
    {@render pipelineId()}
  </div>
  <!-- {:else if !global && pipeline.current.status === 'Suspended'}
  <div class="flex justify-between">
    <div>
      Pipeline is suspended for {formatElapsedTime(
        new Date(pipeline.current.deploymentStatusSince)
      )} since {Dayjs(pipeline.current.deploymentStatusSince).format('MMM D, YYYY h:mm A')}. The
      performance metrics cannot be retrieved.
    </div>
    {@render pipelineId()}
  </div> -->
{:else if !global}
  <div class="flex justify-between">
    <div>Pipeline is running, but has not reported usage telemetry yet</div>
    {@render pipelineId()}
  </div>
{:else}<div class="flex h-full flex-col gap-2 overflow-y-auto overflow-x-clip scrollbar">
    <div class="flex w-full flex-col gap-4">
      <div class="flex flex-wrap gap-4 pt-2">
        <div class="flex flex-col">
          <div class="text-nowrap text-start text-sm">Records Ingested</div>
          <div class="pt-2">
            {formatQty(global.total_input_records)}
          </div>
        </div>
        <div class="flex flex-col">
          <div class="text-nowrap text-start text-sm">
            <span class="hidden sm:inline">Records</span> Processed
          </div>
          <div class="pt-2">
            {formatQty(global.total_processed_records)}
          </div>
        </div>
        <div class="flex flex-col">
          <div class="text-nowrap text-start text-sm">
            <span class="hidden sm:inline">Records</span> Buffered
          </div>
          <div class="pt-2">
            {formatQty(global.buffered_input_records)}
          </div>
        </div>
        {#snippet age()}
          <div class="w-64 pt-2">
            {#if global.start_time > 0}
              Deployed on {Dayjs(global.start_time * 1000).format('MMM D, YYYY h:mm A')}
            {:else}
              Not deployed
            {/if}
          </div>
        {/snippet}
        {#snippet updated()}
          <div class="w-64 text-nowrap pt-2">
            {getDeploymentStatusLabel(pipeline.current.status)} since {Dayjs(
              pipeline.current.deploymentStatusSince
            ).format('MMM D, YYYY h:mm A')}
          </div>
        {/snippet}
        {#if isXl.current}
          <div class="flex flex-col">
            <div class="text-start text-sm">
              Deployment age -

              {#if global.start_time > 0}
                {formatElapsedTime(new Date(global.start_time * 1000))}
              {:else}
                N/A
              {/if}
            </div>
            {@render age()}
          </div>
          <div class="flex flex-col">
            <div class="text-start text-sm">
              Last status update - {formatElapsedTime(
                new Date(pipeline.current.deploymentStatusSince)
              )}
            </div>
            {@render updated()}
          </div>
          {@render pipelineId()}
        {:else}
          <div>
            <Segment
              bind:value={statusTab}
              background="preset-filled-surface-50-950 w-fit flex-none -mt-3 overflow-visible"
              indicatorBg="bg-white-dark shadow"
              indicatorText=""
              border="p-1"
              rounded="rounded"
            >
              <Segment.Item value="age" base="btn cursor-pointer z-[1] px-5 py-4 h-6">
                <div class="text-start text-sm">Age</div>
              </Segment.Item>
              <Segment.Item value="updated" base="btn cursor-pointer z-[1] px-5 py-4 h-6">
                <div class="text-start text-sm">Last status update</div>
              </Segment.Item>
              <Segment.Item value="id" base="">
                <span class="pointer-events-auto">{@render pipelineId()}</span>
              </Segment.Item>
            </Segment>
            {#if statusTab === 'age'}
              {@render age()}
            {:else if statusTab === 'updated'}
              {@render updated()}{/if}
          </div>
        {/if}
      </div>
      <div class="flex w-full flex-col gap-4 xl:flex-row">
        <div class="bg-white-dark relative h-52 w-full max-w-[700px] rounded">
          <PipelineThroughputGraph
            {pipeline}
            metrics={timeSeries}
            refetchMs={1000}
            keepMs={60 * 1000}
          ></PipelineThroughputGraph>
        </div>
        <div class="bg-white-dark relative h-52 w-full max-w-[700px] rounded">
          <PipelineMemoryGraph {pipeline} metrics={timeSeries} refetchMs={1000} keepMs={60 * 1000}
          ></PipelineMemoryGraph>
        </div>
        <div class="bg-white-dark relative h-52 w-full max-w-[700px] rounded">
          <PipelineStorageGraph {pipeline} metrics={timeSeries} refetchMs={1000} keepMs={60 * 1000}
          ></PipelineStorageGraph>
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
