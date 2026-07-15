<script lang="ts" module>
  export const id = 'Performance' as const

  export { label as Label }
</script>

<script lang="ts">
  import { SegmentedControl } from 'common-ui'
  import Dayjs from 'dayjs'
  import PipelineMemoryGraph from '$lib/components/layout/pipelines/PipelineMemoryGraph.svelte'
  import PipelineStorageGraph from '$lib/components/layout/pipelines/PipelineStorageGraph.svelte'
  import PipelineThroughputGraph from '$lib/components/layout/pipelines/PipelineThroughputGraph.svelte'
  import MetricsTables from '$lib/components/pipelines/editor/performance/MetricsTables.svelte'
  import ConnectorErrors, {
    type ConnectorErrorFilter
  } from '$lib/components/pipelines/editor/performance/ConnectorErrors.svelte'
  import CheckpointsStatus from '$lib/components/pipelines/editor/performance/CheckpointsStatus.svelte'
  import { useIsScreenXl } from '$lib/compositions/layout/useIsMobile.svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import { formatDateTime, formatQty } from '$lib/functions/format'
  import { useElapsedTime } from '$lib/compositions/common/useElapsedTime'
  import type { PipelineMetrics } from '$lib/functions/pipelineMetrics'
  import { pushAsCircularBuffer } from '$lib/functions/pipelines/changeStream'
  import { JSONParser } from '@streamparser/json-whatwg'
  import type { ParsedElementInfo } from '@streamparser/json/utils/types/parsedElementInfo.js'
  import { getDeploymentStatusLabel, isMetricsAvailable } from '$lib/functions/pipelines/status'
  import type { CheckpointMetadata, CheckpointStatus } from '$lib/services/manager'
  import type { ExtendedPipeline } from '$lib/services/pipelineManager'
  import type { TimeSeriesEntry } from '$lib/types/pipelineManager'
  import CheckpointsIndicator from './performance/CheckpointsIndicator.svelte'
  import TransactionStatus from './performance/TransactionStatus.svelte'
  import Drawer from '$lib/components/layout/Drawer.svelte'
  import WarningBanner from './WarningBanner.svelte'
  import { sleep } from '$lib/functions/common/promise'

  const RECONNECT_BACKOFF_MS = 1000

  const {
    pipeline,
    metrics,
    deleted = false
  }: {
    pipeline: { current: ExtendedPipeline }
    metrics: { current: PipelineMetrics }
    deleted?: boolean
  } = $props()

  const global = $derived(metrics.current.global)
  const { formatElapsedTime } = useElapsedTime()

  let timeSeries: TimeSeriesEntry[] = $state([])

  let statusTab: 'age' | 'updated' = $state('age')
  const isXl = useIsScreenXl()
  const api = usePipelineManager()

  type DrawerState =
    | {
        kind: 'connector'
        relationName: string
        connectorName: string
        direction: 'input' | 'output'
        filter: ConnectorErrorFilter
      }
    | { kind: 'checkpoints' }

  let openDrawer = $state<DrawerState | null>(null)
  let checkpoints = $state<CheckpointMetadata[]>([])
  let checkpointStatus = $state<CheckpointStatus | null>(null)
  const handleConnectorSelect = (
    relationName: string,
    connectorName: string,
    direction: 'input' | 'output',
    filter: ConnectorErrorFilter
  ) => {
    openDrawer = { kind: 'connector', relationName, connectorName, direction, filter }
  }

  const pipelineName = $derived(pipeline.current.name)
  const metricsStatus = $derived(isMetricsAvailable(pipeline.current.status))
  const metricsAvailable = $derived(metricsStatus === 'yes')
  // When metrics are temporarily unavailable ('missing'), freeze graphs and stats until the pipeline is reachable again.
  const metricsDesired = $derived(metricsStatus === 'yes' || metricsStatus === 'missing')

  // Keep reconnecting to time_series_stream for as long as the tab is mounted and metrics are desired
  // Reconnect on end-of-stream immediately, or with 1s backoff on mid-stream or stream-open errors
  // Clear the timeSeries after reconnecting, when metrics are no longer desired or the pipeline is deleted
  $effect(() => {
    pipelineName
    if (deleted) {
      timeSeries = []
      openDrawer = null
      return
    }
    if (!metricsDesired) {
      timeSeries = []
      openDrawer = null
      checkpoints = []
      return
    }

    const targetPipelineName = pipelineName
    // Start each session with empty stats so a previous pipeline's samples
    // never bleed into the newly selected one.
    timeSeries = []
    let cancelled = false
    let cancelActive: (() => void) | undefined

    const runMetricsStream = async () => {
      // Not routed through `parseStream`: the load shedding is unnecessary for the metrics stream.
      const appendRow = pushAsCircularBuffer(
        () => timeSeries,
        63,
        (v: TimeSeriesEntry) => v
      )
      while (!cancelled) {
        if (!metricsAvailable) {
          await sleep(RECONNECT_BACKOFF_MS)
          continue
        }
        const result = await api.pipelineTimeSeriesStream(targetPipelineName)
        if (cancelled) {
          if (!(result instanceof Error)) {
            result.cancel()
          }
          return
        }
        if (result instanceof Error) {
          // Could not open the stream. Back off briefly, then retry so the graphs recover.
          await sleep(RECONNECT_BACKOFF_MS)
          continue
        }
        const abortCtrl = new AbortController()
        cancelActive = () => {
          abortCtrl.abort()
          result.cancel()
        }

        // Subscribe to the metrics stream, overwrite the previous data only when the first sample is received.
        try {
          let pendingReplace = true
          await result.stream.pipeThrough(new JSONParser({ paths: ['$'], separator: '' })).pipeTo(
            new WritableStream<ParsedElementInfo>({
              write(chunk) {
                const entry = chunk.value as TimeSeriesEntry
                if (pendingReplace) {
                  // The first sample after a reconnect replaces the previous time series window, which may be frozen or stale.
                  timeSeries = [entry]
                  pendingReplace = false
                  return
                }
                appendRow([entry])
              }
            }),
            { signal: abortCtrl.signal }
          )
        } catch (e) {
          // `AbortError` from teardown is intentional, so only log real failures.
          if (!abortCtrl.signal.aborted) {
            console.warn('Pipeline metrics stream error:', e)
          }
        }
        cancelActive = undefined
      }
    }
    runMetricsStream()

    return () => {
      cancelled = true
      cancelActive?.()
    }
  })

  $effect(() => {
    pipelineName
    if (!metricsDesired) {
      checkpoints = []
      checkpointStatus = null
      return
    }
    if (!metricsAvailable) {
      // Keep the last-known metrics and pause polling until the pipeline is reachable again.
      return
    }
    // Poll checkpoint-related endpoints so the UI stays current with ongoing checkpoint activity.
    const fetchCheckpoints = () => {
      api.getPipelineCheckpoints(pipelineName).then((v) => {
        checkpoints = v
      })
      api.getCheckpointStatus(pipelineName).then((v) => {
        checkpointStatus = v
      })
    }
    fetchCheckpoints()
    const interval = setInterval(fetchCheckpoints, 2_000)
    return () => clearInterval(interval)
  })
</script>

{#snippet label()}
  Runtime
{/snippet}

{#if isMetricsAvailable(pipeline.current.status) === 'no'}
  <div class="flex justify-between pt-2 sm:pt-0">
    <div>Pipeline is not running</div>
  </div>
{:else if !global}
  <div class="flex justify-between">
    <div>Pipeline is running, but has not reported usage telemetry yet</div>
  </div>
{:else}<div class="flex h-full">
    <Drawer
      open={!!openDrawer}
      side="right"
      onClose={() => (openDrawer = null)}
      localStorageKey="layout/drawer/pipelinePerformance"
    >
      {#snippet main()}
        <div
          class="-mr-2 scrollbar flex min-w-0 flex-1 flex-col gap-4 overflow-x-clip overflow-y-auto pr-2"
        >
          <div class="flex w-full flex-col gap-4">
            {#if pipeline.current.status === 'Unavailable'}
              <WarningBanner class="rounded!">
                Pipeline has been unavailable for {formatElapsedTime(
                  new Date(pipeline.current.deploymentStatusSince)
                )} since {Dayjs(pipeline.current.deploymentStatusSince).format('MMM D, YYYY h:mm A')}.
                Showing the last known metrics while reconnecting. You can attempt to suspend or shut it
                down.
              </WarningBanner>
            {/if}
            <div class="flex flex-wrap gap-4">
              <div class="mt-1 flex flex-wrap items-center gap-4">
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
                  <div class="w-52 pt-2">
                    {#if global.start_time > 0}
                      On {formatDateTime({ ms: global.start_time * 1000 })}
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
                      onValueChange={(v) => (statusTab = v)}
                      items={[
                        { value: 'age', label: 'Age' },
                        { value: 'updated', label: 'Last status update' }
                      ]}
                      class="-mt-3"
                    />
                    {#if statusTab === 'age'}
                      {@render age()}
                    {:else if statusTab === 'updated'}
                      {@render updated()}{/if}
                  </div>
                {/if}
              </div>
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
                <PipelineMemoryGraph
                  {pipeline}
                  metrics={timeSeries}
                  refetchMs={1000}
                  keepMs={60 * 1000}
                  memoryPressure={global.memory_pressure}
                ></PipelineMemoryGraph>
              </div>
              <div class="bg-white-dark relative h-52 w-full max-w-[700px] rounded">
                <PipelineStorageGraph
                  {pipeline}
                  metrics={timeSeries}
                  refetchMs={1000}
                  keepMs={60 * 1000}
                ></PipelineStorageGraph>
              </div>
            </div>
            <CheckpointsIndicator
              {pipelineName}
              {checkpoints}
              {metrics}
              {checkpointStatus}
              onShowCheckpoints={() => (openDrawer = { kind: 'checkpoints' })}
            />
            <TransactionStatus {metrics} class="w-full"></TransactionStatus>
          </div>
          {#if metrics.current.views.size || metrics.current.tables.size}
            <div class="flex flex-wrap gap-4">
              <MetricsTables {metrics} onConnectorSelect={handleConnectorSelect} />
            </div>
          {/if}
        </div>
      {/snippet}
      {#if openDrawer?.kind === 'connector'}
        <ConnectorErrors
          {pipelineName}
          relationName={openDrawer.relationName}
          connectorName={openDrawer.connectorName}
          direction={openDrawer.direction}
          filter={openDrawer.filter}
          onClose={() => (openDrawer = null)}
        />
      {:else if openDrawer?.kind === 'checkpoints'}
        <CheckpointsStatus
          {checkpoints}
          onClose={() => (openDrawer = null)}
          onCheckpoint={() => api.checkpointPipeline(pipelineName)}
          checkpointInProgress={metrics.current.checkpoint_activity.status !== 'idle'}
        />
      {/if}
    </Drawer>
  </div>
{/if}
