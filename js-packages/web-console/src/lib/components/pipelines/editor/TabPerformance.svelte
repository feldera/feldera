<script lang="ts">
  import { SegmentedControl } from '@skeletonlabs/skeleton-svelte'
  import { format } from 'd3-format'
  import Dayjs from 'dayjs'
  import PipelineMemoryGraph from '$lib/components/layout/pipelines/PipelineMemoryGraph.svelte'
  import PipelineStorageGraph from '$lib/components/layout/pipelines/PipelineStorageGraph.svelte'
  import PipelineThroughputGraph from '$lib/components/layout/pipelines/PipelineThroughputGraph.svelte'
  import MetricsTables from '$lib/components/pipelines/editor/MetricsTables.svelte'
  import { useIsScreenXl } from '$lib/compositions/layout/useIsMobile.svelte'
  import {
    type PipelineManagerApi,
    usePipelineManager
  } from '$lib/compositions/usePipelineManager.svelte'
  import { formatDateTime, useElapsedTime } from '$lib/functions/format'
  import type { PipelineMetrics } from '$lib/functions/pipelineMetrics'
  import {
    CustomJSONParserTransformStream,
    parseCancellable,
    pushAsCircularBuffer
  } from '$lib/functions/pipelines/changeStream'
  import { getDeploymentStatusLabel, isMetricsAvailable } from '$lib/functions/pipelines/status'
  import type { ExtendedPipeline } from '$lib/services/pipelineManager'
  import type { TimeSeriesEntry } from '$lib/types/pipelineManager'

  const formatQty = (v: number) => format(',.0f')(v)

  const {
    pipeline,
    metrics
  }: { pipeline: { current: ExtendedPipeline }; metrics: { current: PipelineMetrics } } = $props()

  const global = $derived(metrics.current.global)
  const { formatElapsedTime } = useElapsedTime()

  let timeSeries: TimeSeriesEntry[] = $state([])

  let statusTab: 'age' | 'updated' = $state('age')
  const isXl = useIsScreenXl()
  const api = usePipelineManager()

  let cancelStream: (() => void) | undefined

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

  const pipelineName = $derived(pipeline.current.name)
  const metricsAvailable = $derived(isMetricsAvailable(pipeline.current.status) === 'yes')

  $effect(() => {
    pipelineName
    if (!metricsAvailable) {
      endMetricsStream()
      return
    }
    $effect.root(() => {
      if (cancelStream) {
        // Avoid redundant cleanup on first start
        endMetricsStream()
      }
      setTimeout(() => startMetricsStream(api, pipelineName), 100)
    })
    return () => {
      endMetricsStream()
    }
  })
</script>

{#if isMetricsAvailable(pipeline.current.status) === 'no'}
  <div class="flex justify-between pt-2 sm:pt-0">
    <div>Pipeline is not running</div>
  </div>
{:else if pipeline.current.status === 'Unavailable'}
  <div class="flex justify-between">
    <div>
      Pipeline is unavailable for {formatElapsedTime(
        new Date(pipeline.current.deploymentStatusSince)
      )} since {Dayjs(pipeline.current.deploymentStatusSince).format('MMM D, YYYY h:mm A')}. You can
      attempt to suspend or shut it down.
    </div>
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
  </div>
{:else}<div class="scrollbar flex h-full flex-col gap-4 overflow-x-clip overflow-y-auto">
    <div class="flex w-full flex-col gap-4">
      <div class="flex flex-wrap gap-4 pt-2">
        <div class="flex flex-col">
          <div class="text-start text-sm text-nowrap">Records Ingested</div>
          <div class="pt-2">
            {formatQty(global.total_input_records)}
          </div>
        </div>
        <div class="flex flex-col">
          <div class="text-start text-sm text-nowrap">Records Processed</div>
          <div class="pt-2">
            {formatQty(global.total_processed_records)}
          </div>
        </div>
        <div class="flex flex-col">
          <div class="text-start text-sm text-nowrap">Records Buffered</div>
          <div class="pt-2">
            {formatQty(global.buffered_input_records)}
          </div>
        </div>
        {#snippet age()}
          <div class="w-64 pt-2">
            {#if global.start_time > 0}
              Deployed on {formatDateTime({ ms: global.start_time * 1000 })}
            {:else}
              Not deployed
            {/if}
          </div>
        {/snippet}
        {#snippet updated()}
          <div class="w-64 pt-2 text-nowrap">
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
        {:else}
          <div>
            <SegmentedControl
              value={statusTab}
              onValueChange={(e) => (statusTab = e.value as typeof statusTab)}
            >
              <SegmentedControl.Label />
              <SegmentedControl.Control
                class="-mt-3 w-fit flex-none overflow-visible rounded preset-filled-surface-50-950 p-1"
              >
                <SegmentedControl.Indicator class="bg-white-dark shadow" />
                <SegmentedControl.Item value="age" class="z-1 btn h-6 cursor-pointer px-5 py-4">
                  <SegmentedControl.ItemText class="text-surface-950-50">
                    <div class="text-start text-sm">Age</div>
                  </SegmentedControl.ItemText>
                  <SegmentedControl.ItemHiddenInput />
                </SegmentedControl.Item>
                <SegmentedControl.Item value="updated" class="z-1 btn h-6 cursor-pointer px-5 py-4">
                  <SegmentedControl.ItemText class="text-surface-950-50">
                    <div class="text-start text-sm">Last status update</div>
                  </SegmentedControl.ItemText>
                  <SegmentedControl.ItemHiddenInput />
                </SegmentedControl.Item>
              </SegmentedControl.Control>
            </SegmentedControl>
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
    {#if metrics.current.views.size || metrics.current.tables.size}
      <div class="flex flex-wrap gap-4">
        <MetricsTables {metrics} />
      </div>
    {/if}
  </div>
{/if}
