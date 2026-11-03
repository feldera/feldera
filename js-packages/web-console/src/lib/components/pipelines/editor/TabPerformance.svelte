<script lang="ts" module>
  export const id = 'Performance' as const

  export { label as Label }
</script>

<script lang="ts">
  import type { ConnectorErrorFilter } from 'common-ui/connectorMetrics'
  import { formatDateTime, formatQty } from 'common-lib/format'
  import { sleep } from 'common-lib/promise'
  import { ConnectorMetricsTables, Drawer, SegmentedControl, useIsScreenXl } from 'common-ui'
  import Dayjs from 'dayjs'
  import { usePermission } from '#lib/compositions/usePermission.svelte.ts'
  import { getPipelineAction } from '#lib/compositions/usePipelineAction.svelte.ts'
  import { usePipelineActionCallbacks } from '#lib/compositions/pipelines/usePipelineActionCallbacks.svelte.ts'
  import { useToast } from '#lib/compositions/useToastNotification.ts'
  import { useDarkMode } from '#lib/compositions/useDarkMode.svelte.ts'
  import NotRunningLight from '#assets/illustrations/pipeline-not-running-light.svg?component'
  import NotRunningDark from '#assets/illustrations/pipeline-not-running-dark.svg?component'
  import PipelineMemoryGraph from '#lib/components/layout/pipelines/PipelineMemoryGraph.svelte'
  import PipelineStorageGraph from '#lib/components/layout/pipelines/PipelineStorageGraph.svelte'
  import PipelineThroughputGraph from '#lib/components/layout/pipelines/PipelineThroughputGraph.svelte'
  import ConnectorErrors from '#lib/components/pipelines/editor/performance/ConnectorErrors.svelte'
  import CheckpointsStatus from '#lib/components/pipelines/editor/performance/CheckpointsStatus.svelte'
  import { usePipelineManager } from '#lib/compositions/usePipelineManager.svelte.ts'
  import { useElapsedTime } from '#lib/compositions/common/useElapsedTime.ts'
  import { staleSampleCount, type PipelineMetrics } from '#lib/functions/pipelineMetrics.ts'
  import { JSONParser } from '@streamparser/json-whatwg'
  import type { ParsedElementInfo } from '@streamparser/json/utils/types/parsedElementInfo.js'
  import { getDeploymentStatusLabel, isMetricsAvailable } from '#lib/functions/pipelines/status.ts'
  import type { CheckpointMetadata, CheckpointStatus } from '#lib/services/manager/index.ts'
  import type { ExtendedPipeline } from '#lib/services/pipelineManager.ts'
  import type { TimeSeriesEntry } from '#lib/types/pipelineManager.ts'
  import CheckpointsIndicator from './performance/CheckpointsIndicator.svelte'
  import CommitProgressIndicator from './performance/CommitProgressIndicator.svelte'
  import WarningBanner from './WarningBanner.svelte'

  const RECONNECT_BACKOFF_MS = 1000
  /** Time span the graphs plot. */
  const GRAPH_WINDOW_MS = 60 * 1000
  const RETAIN_MS = GRAPH_WINDOW_MS + 2000
  /**
   * Ceiling on samples kept in `timeSeries`, reached only if their timestamps
   * stop advancing. Well above the rate any deployment reports: a 16-host
   * pipeline sampling every second fills a fifth of it.
   */
  const MAX_SAMPLES = 5000

  const {
    pipeline,
    metrics,
    deleted = false,
    unsavedChanges = false
  }: {
    pipeline: { current: ExtendedPipeline }
    metrics: { current: PipelineMetrics }
    deleted?: boolean
    /** Unsaved code changes; the Start button below waits for them to be saved. */
    unsavedChanges?: boolean
  } = $props()

  const global = $derived(metrics.current.global)
  const { formatElapsedTime } = useElapsedTime()

  let timeSeries: TimeSeriesEntry[] = $state([])
  // Metrics stream lifecycle. 'connecting' is the initial sate and never happens again,
  // stream can be 'interrupted' every time the 'live' conection drops
  let metricsStreamState = $state<'connecting' | 'live' | 'interrupted'>('connecting')

  let statusTab: 'age' | 'updated' = $state('age')
  const isXl = useIsScreenXl()
  const darkMode = useDarkMode()

  // The empty state's Start button starts the pipeline the way the toolbar's Start does,
  // and is offered in the same cases: a stopped pipeline and permission to run it.
  const canExec = usePermission('exec:pipeline')
  const { postPipelineAction } = getPipelineAction()
  const pipelineActionCallbacks = usePipelineActionCallbacks()
  const { toastError } = useToast()
  const canStart = $derived(!deleted && pipeline.current.status === 'Stopped' && canExec.allowed)
  const startPipeline = async () => {
    const { waitFor } = await postPipelineAction(pipeline.current.name, 'start', {
      onPausedReady: async (pipelineName: string) => {
        const cbs = pipelineActionCallbacks.getAll(pipelineName, 'start_paused')
        await Promise.allSettled(cbs.map((cb) => cb(pipelineName)))
      }
    })
    waitFor().catch(toastError('Waiting for pipeline to start'))
  }
  const api = usePipelineManager()
  // Background polling races pipeline shutdown/startup, so don't show toast popups
  // when the expected "pipeline not running" errors happen while reconnecting.
  const pollingApi = api.silence((e) =>
    ['PipelineInteractionNotDeployed', 'PipelineUnavailable'].includes(e?.error_code)
  )

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
  // Show the reconnect banner only after a live stream has dropped, never during the first connect.
  const metricsStreamInterrupted = $derived(
    metricsAvailable && metricsStreamState === 'interrupted'
  )

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
    metricsStreamState = 'connecting'
    let cancelled = false
    let cancelActive: (() => void) | undefined

    const runMetricsStream = async () => {
      // Not routed through `parseStream`: the load shedding is unnecessary for the metrics stream.
      // Retention goes by sample age, not by sample count: a multihost pipeline
      // reports one sample per host per tick, so the sample rate depends on the
      // deployment and a count-based buffer plots a shorter span the more hosts
      // the pipeline runs on.
      const appendSample = (sample: TimeSeriesEntry) => {
        timeSeries.push(sample)
        const stale = staleSampleCount(timeSeries, RETAIN_MS, MAX_SAMPLES)
        if (stale > 0) {
          timeSeries.splice(0, stale)
        }
      }
      while (!cancelled) {
        if (!metricsAvailable) {
          await sleep(RECONNECT_BACKOFF_MS)
          continue
        }
        const result = await pollingApi.pipelineTimeSeriesStream(targetPipelineName)
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
        metricsStreamState = 'live'

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
                appendSample(entry)
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
        metricsStreamState = 'interrupted'
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
      pollingApi.getPipelineCheckpoints(pipelineName).then((v) => {
        checkpoints = v
      })
      pollingApi.getCheckpointStatus(pipelineName).then((v) => {
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
  <div class="flex h-full flex-col items-center justify-center gap-2 p-4 text-center">
    {#if darkMode.current === 'dark'}
      <NotRunningDark viewBox="40 60 160 60" class="h-[60px] w-[160px]"></NotRunningDark>
    {:else}
      <NotRunningLight viewBox="40 60 160 60" class="h-[60px] w-[160px]"></NotRunningLight>
    {/if}
    <div class="mt-4 text-lg text-surface-950-50">Pipeline isn’t running</div>
    <div class="text-surface-600-400">Start the pipeline to see runtime metrics and activity.</div>
    {#if canStart}
      <button
        class="mt-3 btn h-8 preset-outlined-surface-200-800 px-3"
        disabled={unsavedChanges}
        onclick={startPipeline}
      >
        <span class="fd fd-play text-[14px]"></span>
        Start pipeline
      </button>
      {#if unsavedChanges}
        <div class="text-sm text-surface-600-400">Save your changes to start the pipeline.</div>
      {/if}
    {/if}
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
          class="-mr-2 scrollbar flex h-full min-w-0 flex-1 flex-col gap-4 overflow-x-clip overflow-y-auto pr-2 pb-10 pl-3"
        >
          <div class="flex w-full flex-col gap-4">
            {#if pipeline.current.status === 'Unavailable'}
              <WarningBanner class="rounded!">
                Pipeline has been unavailable for {formatElapsedTime(
                  new Date(pipeline.current.deploymentStatusSince)
                )} since {Dayjs(pipeline.current.deploymentStatusSince).format(
                  'MMM D, YYYY h:mm A'
                )}. Showing the last known metrics while reconnecting. You can attempt to suspend or
                shut it down.
              </WarningBanner>
            {:else if metricsStreamInterrupted}
              <WarningBanner class="rounded!">
                Not receiving live metrics. Attempting to reconnect...
              </WarningBanner>
            {/if}
            <div class="flex flex-wrap gap-4">
              <div class="mt-4.5 flex flex-wrap items-center gap-4 xl:gap-x-10">
                <div class="flex flex-col">
                  <div class="text-start text-sm text-nowrap text-surface-600-400">
                    Records Ingested
                  </div>
                  <div class="pt-1 max-xl:text-[12px]">
                    {formatQty(global.total_input_records)}
                  </div>
                </div>
                <div class="flex flex-col">
                  <div class="text-start text-sm text-nowrap text-surface-600-400">
                    Records Processed
                  </div>
                  <div class="pt-1 max-xl:text-[12px]">
                    {formatQty(global.total_processed_records)}
                  </div>
                </div>
                <div class="flex flex-col">
                  <div class="text-start text-sm text-nowrap text-surface-600-400">
                    Records Buffered
                  </div>
                  <div class="pt-1 max-xl:text-[12px]">
                    {formatQty(global.buffered_input_records)}
                  </div>
                </div>
                {#snippet age()}
                  <div class="w-52 pt-1 max-xl:text-[12px]">
                    {#if global.start_time > 0}
                      On {formatDateTime({ ms: global.start_time * 1000 })}
                    {:else}
                      Not deployed
                    {/if}
                  </div>
                {/snippet}
                {#snippet updated()}
                  <div class="w-64 pt-1 text-nowrap max-xl:text-[12px]">
                    {getDeploymentStatusLabel(pipeline.current.status)} since {Dayjs(
                      pipeline.current.deploymentStatusSince
                    ).format('MMM D, YYYY h:mm A')}
                  </div>
                {/snippet}
                {#if isXl.current}
                  <div class="flex flex-col">
                    <div class="text-start text-sm text-surface-600-400">
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
                    <div class="text-start text-sm text-surface-600-400">
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
                      class="-mt-1"
                    />
                    {#if statusTab === 'age'}
                      {@render age()}
                    {:else if statusTab === 'updated'}
                      {@render updated()}{/if}
                  </div>
                {/if}
              </div>
            </div>
            <div class="flex w-full flex-col gap-2 xl:flex-row">
              <div
                class="bg-white-dark relative h-52 w-full max-w-[700px] rounded-lg border border-line dark:border-[#3f3f3f]"
              >
                <PipelineThroughputGraph
                  {pipeline}
                  metrics={timeSeries}
                  refetchMs={1000}
                  keepMs={GRAPH_WINDOW_MS}
                ></PipelineThroughputGraph>
              </div>
              <div
                class="bg-white-dark relative h-52 w-full max-w-[700px] rounded-lg border border-line dark:border-[#3f3f3f]"
              >
                <PipelineMemoryGraph
                  {pipeline}
                  metrics={timeSeries}
                  refetchMs={1000}
                  keepMs={GRAPH_WINDOW_MS}
                  memoryPressure={global.memory_pressure}
                ></PipelineMemoryGraph>
              </div>
              <div
                class="bg-white-dark relative h-52 w-full max-w-[700px] rounded-lg border border-line dark:border-[#3f3f3f]"
              >
                <PipelineStorageGraph
                  {pipeline}
                  metrics={timeSeries}
                  refetchMs={1000}
                  keepMs={GRAPH_WINDOW_MS}
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
            <CommitProgressIndicator
              {metrics}
              class="bg-white-dark w-full max-w-[1540px] rounded p-2"
            ></CommitProgressIndicator>
          </div>
          {#if metrics.current.views.size || metrics.current.tables.size}
            <div class="flex flex-wrap gap-4">
              <ConnectorMetricsTables {metrics} onConnectorSelect={handleConnectorSelect} />
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
