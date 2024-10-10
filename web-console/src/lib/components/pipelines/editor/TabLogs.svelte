<script lang="ts" module>
  let streams: Record<
    string,
    {
      rows: string[]
      stream: { open: ReadableStream<Uint8Array>; stop: () => void } | { closed: {} }
    }
  > = {}
  let getStreams = $state(() => streams)
</script>

<script lang="ts">
  import LogsStreamList from '$lib/components/pipelines/editor/LogsStreamList.svelte'

  import { parseUTF8AsTextLines } from '$lib/functions/pipelines/changeStream'
  import { isPipelineIdle } from '$lib/functions/pipelines/status'
  import { pipelineLogsStream, type ExtendedPipeline } from '$lib/services/pipelineManager'

  let { pipeline }: { pipeline: { current: ExtendedPipeline } } = $props()
  let pipelineName = $derived(pipeline.current.name)

  $effect.pre(() => {
    if (!streams[pipelineName]) {
      streams[pipelineName] = {
        stream: { closed: {} },
        rows: []
      }
    }
  })
  $effect(() => {
    queueMicrotask(() => {
      if ('open' in streams[pipelineName].stream) {
        return
      }
      if (isPipelineIdle(pipeline.current.status)) {
        return
      }
      startStream(pipelineName)
    })
  })

  const startStream = (pipelineName: string) => {
    if ('open' in streams[pipelineName].stream) {
      return
    }
    pipelineLogsStream(pipelineName).then((result) => {
      if (result instanceof Error) {
        return
      }
      const startTimestamp = Date.now()
      const { cancel } = parseUTF8AsTextLines(
        result,
        (line) => streams[pipelineName].rows.push(line),
        () => {
          streams[pipelineName] = { stream: { closed: {} }, rows: streams[pipelineName].rows }
          if (
            typeof pipeline.current.status === 'string' &&
            ['Shutdown', 'ShuttingDown'].includes(pipeline.current.status)
          ) {
            return
          }
          tryRestartStream(pipelineName, startTimestamp)
        }
      )
      streams[pipelineName] = { stream: { open: result, stop: cancel }, rows: [] }
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

  // Start a stream unless one is already open
  // const tryStartStream = (pipelineName: string) => {
  //   if ('open' in streams[pipelineName].stream) {
  //     return
  //   }
  //   streams[pipelineName] = {
  //     stream: { closed: {} },
  //     rows: streams[pipelineName].rows
  //   }
  //   startStream(pipelineName)
  // }

  // $effect(() => {
  //   pipelineName
  //   queueMicrotask(() => {
  //     tryStartStream(pipelineName)
  //   })
  // })

  let previousStatus = $state(pipeline.current.status)
  $effect(() => {
    pipelineName
    queueMicrotask(() => {
      previousStatus = pipeline.current.status
    })
  })
  $effect(() => {
    if ('open' in streams[pipelineName]) {
      return
    }
    if (previousStatus === pipeline.current.status) {
      return
    }
    if (
      (typeof pipeline.current.status === 'string' &&
        ['Initializing', 'Running', 'Paused'].includes(pipeline.current.status)) ||
      (typeof pipeline.current.status === 'object' && 'PipelineError' in pipeline.current.status)
    ) {
      startStream(pipelineName)
    }
    previousStatus = pipeline.current.status
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
</script>

<LogsStreamList rows={getStreams()[pipelineName].rows}></LogsStreamList>
