<script lang="ts" module>
  let streams: Record<
    string,
    {
      firstRowIndex: number
      rows: string[]
      rowBoundaries: number[]
      totalSkippedBytes: number
      stream: { open: ReadableStream<Uint8Array>; stop: () => void } | { closed: {} }
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
    parseCancellable,
    pushAsCircularBuffer,
    SplitNewlineTransformStream
  } from '$lib/functions/pipelines/changeStream'
  import { pipelineLogsStream, type ExtendedPipeline } from '$lib/services/pipelineManager'
  import { usePipelineActionCallbacks } from '$lib/compositions/pipelines/usePipelineActionCallbacks.svelte'
  import { untrack } from 'svelte'

  let { pipeline }: { pipeline: { current: ExtendedPipeline } } = $props()
  let pipelineName = $derived(pipeline.current.name)
  let pipelineStatus = $derived(pipeline.current.status)

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
  const startStream = (pipelineName: string) => {
    if ('open' in streams[pipelineName].stream) {
      return
    }
    pipelineLogsStream(pipelineName).then((result) => {
      if (result instanceof Error) {
        return
      }
      const startTimestamp = Date.now()
      const { cancel } = parseCancellable(
        result,
        {
          pushChanges: (changes: string[]) => {
            const droppedNum = pushAsCircularBuffer(
              () => streams[pipelineName].rows,
              bufferSize,
              (v: string) => v
            )(changes)
            streams[pipelineName].firstRowIndex += droppedNum
          },
          onParseEnded: (reason) => {
            streams[pipelineName].stream = { closed: {} }
            if (
              reason === 'cancelled' ||
              (typeof pipeline.current.status === 'string' &&
                ['Shutdown', 'ShuttingDown'].includes(pipeline.current.status))
            ) {
              return
            }
            tryRestartStream(pipelineName, startTimestamp)
          },
          onBytesSkipped(bytes) {
            streams[pipelineName].totalSkippedBytes += bytes
          }
        },
        new SplitNewlineTransformStream(),
        {
          bufferSize: 16 * 1024 * 1024
        }
      )
      streams[pipelineName] = {
        firstRowIndex: 0,
        stream: { open: result.stream, stop: cancel },
        rows: [],
        rowBoundaries: [],
        totalSkippedBytes: 0
      }
    })
  }

  // Start stream unless it ended less than retryAllowedSinceDelayMs ago
  const tryRestartStream = (pipelineName: string, startTimestamp: number) => {
    const retryAllowedSinceDelayMs = 2000
    if (startTimestamp + retryAllowedSinceDelayMs > Date.now()) {
      return
    }
    startStream(pipelineName)
  }

  $effect(() => {
    pipelineStatus
    if ('open' in streams[pipelineName]) {
      return
    }
    untrack(() => {
      if (
        (typeof pipelineStatus === 'string' &&
          ['Initializing', 'Running', 'Paused'].includes(pipelineStatus)) ||
        (typeof pipelineStatus === 'object' && 'PipelineError' in pipelineStatus)
      ) {
        startStream(pipelineName)
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
</script>

{#key pipelineName}
  <LogsStreamList logs={getStreams()[pipelineName]}></LogsStreamList>
{/key}
