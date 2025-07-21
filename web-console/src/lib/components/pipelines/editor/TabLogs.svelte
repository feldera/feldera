<script lang="ts" module>
  let streams: Record<
    string,
    {
      firstRowIndex: number
      rows: string[]
      rowBoundaries: number[]
      totalSkippedBytes: number
      stream:
        | { open: ReadableStream<Uint8Array>; stop: () => void }
        | { closed: {} }
        | { closed: {}; cancelRetry: () => void; retryAtTimestamp: number }
    }
  > = {}
  let getStreams = $state(() => streams)
  const pipelineActionCallbacks = usePipelineActionCallbacks()
  const dropLogHistory = async (pipelineName: string) => {
    if ('open' in streams[pipelineName].stream) {
      streams[pipelineName].stream.stop()
    }
    delete streams[pipelineName]
  }
</script>

<script lang="ts">
  import LogsStreamList from '$lib/components/pipelines/editor/LogsStreamList.svelte'

  import {
    BatchingWritableStream,
    pushAsCircularBuffer,
    SplitNewlineTransformStream
  } from '$lib/functions/pipelines/changeStream'
  import { type ExtendedPipeline, type PipelineStatus } from '$lib/services/pipelineManager'
  import { usePipelineActionCallbacks } from '$lib/compositions/pipelines/usePipelineActionCallbacks.svelte'
  import { untrack } from 'svelte'
  import WarningBanner from '$lib/components/pipelines/editor/WarningBanner.svelte'
  import { useInterval } from '$lib/compositions/common/useInterval.svelte'
  import Dayjs from 'dayjs'
  import { unionName, type NamesInUnion } from '$lib/functions/common/union'
  import { match } from 'ts-pattern'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'

  let { pipeline }: { pipeline: { current: ExtendedPipeline } } = $props()
  let pipelineName = $derived(pipeline.current.name)

  let pipelineStatusName = $derived(unionName(pipeline.current.status))

  $effect.pre(() => {
    if (!streams[pipelineName]) {
      streams[pipelineName] = {
        firstRowIndex: 0,
        stream: { closed: {} },
        rows: [],
        rowBoundaries: [],
        totalSkippedBytes: 0
      }
    }
  })
  $effect(() => {
    pipelineName // Reactive dependency only needed when closing the previous stream when switching pipelines
    {
      // Close log stream when leaving log tab, or switching to another pipeline
      let oldPipelineName = pipelineName
      return () => {
        if (streams[oldPipelineName] && 'open' in streams[oldPipelineName].stream) {
          streams[oldPipelineName].stream.stop()
          return
        }
      }
    }
  })
  const bufferSize = 10000

  const areLogsExpected = (pipelineStatusName: NamesInUnion<PipelineStatus>) =>
    match(pipelineStatusName)
      .with(
        'Queued',
        'CompilingSql',
        'SqlCompiled',
        'CompilingRust',
        'Preparing',
        'SqlError',
        'RustError',
        'SystemError',
        'Stopped',
        'Stopping',
        // =============
        'Provisioning',
        'Initializing',
        'Running',
        'Pausing',
        'Paused',
        'Suspending',
        'Resuming',
        'Unavailable',
        () => true
      )
      .exhaustive()

  const api = usePipelineManager()
  const startStream = (pipelineName: string) => {
    if ('open' in streams[pipelineName].stream) {
      return
    }
    api.pipelineLogsStream(pipelineName).then((result) => {
      if (result instanceof Error) {
        streams[pipelineName].stream = { closed: {} }
        streams[pipelineName].rows.push(result.message)
        tryRestartStream(pipelineName, 5000)
        return
      }
      const sink = new BatchingWritableStream({
        pushChanges: (changes: string[]) => {
          const droppedNum = pushAsCircularBuffer(
            () => streams[pipelineName].rows,
            bufferSize,
            (v: string) => v
          )(changes)
          streams[pipelineName].firstRowIndex += droppedNum.offset
        },
        onParseEnded: (reason) => {
          streams[pipelineName].stream = { closed: {} }
          if (reason === 'cancelled' || !areLogsExpected(pipelineStatusName)) {
            return
          }
          tryRestartStream(pipelineName, 5000)
        }
        // onBytesSkipped(bytes) {
        //   streams[pipelineName].totalSkippedBytes += bytes
        // }
      })
      result.stream.pipeThrough(new SplitNewlineTransformStream()).pipeTo(sink)
      streams[pipelineName] = {
        firstRowIndex: 0,
        stream: {
          open: result.stream,
          stop: () => {
            try {
              result.cancel()
            } catch {}
            sink.abort()
          }
        },
        rows: [],
        rowBoundaries: [],
        totalSkippedBytes: 0
      }
      getStreams = () => streams
    })
  }

  // Start stream unless it ended less than retryAllowedSinceDelayMs ago
  const tryRestartStream = (pipelineName: string, delayMs: number) => {
    if ('cancelRetry' in streams[pipelineName].stream) {
      return
    }
    const timeout = setTimeout(() => startStream(pipelineName), delayMs)
    streams[pipelineName].stream = {
      closed: {},
      cancelRetry: () => {
        clearTimeout(timeout)
        streams[pipelineName].stream = { closed: {} }
      },
      retryAtTimestamp: Date.now() + delayMs
    }
  }

  $effect.pre(() => {
    pipelineStatusName
    untrack(() => {
      if ('cancelRetry' in streams[pipelineName].stream) {
        streams[pipelineName].stream.cancelRetry()
      }
      if (areLogsExpected(pipelineStatusName)) {
        startStream(pipelineName)
        return
      }
    })
  })

  // Trigger update to display the latest rows when switching to another pipeline
  $effect(() => {
    pipelineName
    getStreams = () => streams
  })
  $effect(() => {
    const interval = setInterval(() => (getStreams = () => streams), 300)
    return () => clearInterval(interval)
  })
  $effect(() => {
    untrack(() => pipelineActionCallbacks.add('', 'delete', dropLogHistory))
    return () => {
      pipelineActionCallbacks.remove('', 'delete', dropLogHistory)
    }
  })
  let stream = $derived(getStreams()[pipelineName].stream)
  const now = useInterval(() => new Date(), 1000, 1000 - (Date.now() % 1000))
</script>

<div class="relative flex h-full flex-1 flex-col rounded">
  {#if 'closed' in stream}
    {#if 'retryAtTimestamp' in stream && pipelineStatusName !== 'Preparing' && pipelineStatusName !== 'Provisioning' && pipelineStatusName !== 'Initializing'}
      <WarningBanner>
        {@const seconds = Math.floor(
          Dayjs.duration(stream.retryAtTimestamp - now.current.valueOf()).asSeconds()
        )}
        Connection to logs stream lost.
        {#if seconds > 0}Retrying in
          {seconds}s...
        {:else}
          Retrying now...
        {/if}
      </WarningBanner>
    {:else if !areLogsExpected(pipelineStatusName)}
      {#if getStreams()[pipelineName].rows.length}
        <WarningBanner variant="info">
          Displaying log history from the last pipeline run. When the pipeline is started again this
          history will be cleared.
        </WarningBanner>
      {:else}
        <WarningBanner variant="info">
          There are no logs available as the pipeline is shutdown.
        </WarningBanner>
      {/if}
    {/if}
  {/if}
  {#key pipelineName}
    <LogsStreamList logs={getStreams()[pipelineName]}></LogsStreamList>
  {/key}
</div>
