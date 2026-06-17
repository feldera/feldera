<script lang="ts" module>
  const streams: Record<
    string,
    {
      firstRowIndex: number
      rows: string[]
      rowBoundaries: number[]
      totalSkippedBytes: number
      stream:
        | { cancelFetch: () => void }
        | { open: ReadableStream<Uint8Array>; stop: () => void }
        | { closed: {} }
        | { closed: {}; cancelRetry: () => void; retryAtTimestamp: number }
    }
  > = {}
  let getStreams = new Ref(streams)
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
  import { emptySearchState, type SearchState } from 'common-ui'

  import {
    newlineTextDecoder,
    parseStream,
    pushAsCircularBuffer
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
  import { Ref } from '$lib/compositions/ref.svelte'

  let {
    pipeline,
    deleted = false,
    logSearch = emptySearchState,
    onLogSearchShortcut
  }: {
    pipeline: { current: ExtendedPipeline }
    deleted?: boolean
    logSearch?: SearchState
    /** Invoked when the user presses Ctrl-F / Cmd-F inside the log list — the monitoring
     *  panel uses this to focus its search input. */
    onLogSearchShortcut?: () => void
  } = $props()
  let pipelineName = $derived(pipeline.current.name)

  let pipelineStatusName = $derived(unionName(pipeline.current.status))

  let pipelineLogs = $derived.by(() => {
    if (!streams[pipelineName]) {
      streams[pipelineName] = {
        firstRowIndex: 0,
        stream: { closed: {} },
        rows: [],
        rowBoundaries: [],
        totalSkippedBytes: 0
      }
    }
    return getStreams.current[pipelineName]
  })

  $effect(() => {
    pipelineName // Reactive dependency only needed when closing the previous stream when switching pipelines
    untrack(() => {
      if (!deleted) {
        startStream(pipelineName, 0)
      }
    })
    // Close log stream when leaving log tab, switching to another pipeline, or when readonly
    let oldPipelineName = pipelineName
    return () => {
      stopLogStream(oldPipelineName)
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
        'Suspending',
        'Suspended',
        'Standby',
        'Bootstrapping',
        'Replaying',
        'AwaitingApproval',
        'Running',
        'Pausing',
        'Paused',
        'Resuming',
        'Unavailable',
        () => true
      )
      .exhaustive()

  const api = usePipelineManager()
  const startStream = (pipelineName: string, attempts: number) => {
    if ('open' in streams[pipelineName].stream || 'cancelFetch' in streams[pipelineName].stream) {
      return
    }
    const abortController = new AbortController()
    streams[pipelineName].stream = {
      cancelFetch: () => {
        abortController.abort()
        streams[pipelineName].stream = { closed: {} }
      }
    }
    api.pipelineLogsStream(pipelineName, { signal: abortController.signal }).then((result) => {
      if (!streams[pipelineName]) {
        return
      }
      if (streams[pipelineName].stream && 'closed' in streams[pipelineName].stream) {
        // The stream was cancelled, so we shouldn't re-try it
        return
      }
      if (result instanceof Error) {
        streams[pipelineName].stream = { closed: {} }
        streams[pipelineName].rows.push(result.message)
        const isServerOverloaded = (result.cause as { response: Response }).response.status === 503
        tryRestartStream(pipelineName, isServerOverloaded ? attempts + 1 : 0)
        return
      }
      // Replace the previous connection's buffer only once fresh data is actually in hand,
      // not the moment the fetch resolves. Otherwise a reconnect (scroll-resume, retry) blanks
      // the view between connecting and the first byte. Until then the prior rows stay visible.
      let freshConnection = true
      const { cancel } = parseStream<string>(
        result,
        newlineTextDecoder({
          bufferSize: 16 * 1024 * 1024,
          onBytesSkipped: (bytes) => {
            streams[pipelineName].totalSkippedBytes += bytes
          }
        }),
        {
          pushChanges: (changes: string[]) => {
            if (freshConnection) {
              freshConnection = false
              streams[pipelineName].rows = []
              streams[pipelineName].firstRowIndex = 0
              streams[pipelineName].rowBoundaries = []
              streams[pipelineName].totalSkippedBytes = 0
            }
            const droppedNum = pushAsCircularBuffer(
              () => streams[pipelineName].rows,
              bufferSize,
              (v: string) => v
            )(changes)
            streams[pipelineName].firstRowIndex += droppedNum
          },
          onParseEnded: (reason) => {
            const current = streams[pipelineName]?.stream
            // Ignore a callback from a stream we've already replaced: scroll-pause can stop
            // this stream and scroll-resume can open a new one before this 'cancelled' callback
            // lands. Acting on it would clobber the live stream's handle. Identify "still mine"
            // by the open ReadableStream reference.
            if (!current || !('open' in current) || current.open !== result.stream) {
              return
            }
            streams[pipelineName].stream = { closed: {} }
            if (reason === 'cancelled' || !areLogsExpected(pipelineStatusName)) {
              return
            }
            tryRestartStream(pipelineName, 0)
          }
        }
      )
      // Keep the existing rows in place — only swap in the live stream handle. The buffer is
      // cleared on the first `pushChanges` above, so the view stays populated until then.
      streams[pipelineName].stream = { open: result.stream, stop: cancel }
      getStreams.current = streams
    })
  }
  const backoffDelaysMs = [5, 5, 15, 30, 60].map((s) => s * 1000)
  const getDelayMs = (attempts: number) => backoffDelaysMs.at(attempts) ?? backoffDelaysMs.at(-1)!
  // Start stream unless it ended less than retryAllowedSinceDelayMs ago
  const tryRestartStream = (pipelineName: string, attempts: number) => {
    if (deleted) return
    if ('cancelRetry' in streams[pipelineName].stream) {
      return
    }
    const delayMs = getDelayMs(attempts)
    const timeout = setTimeout(() => startStream(pipelineName, attempts), delayMs)
    streams[pipelineName].stream = {
      closed: {},
      cancelRetry: () => {
        clearTimeout(timeout)
        streams[pipelineName].stream = { closed: {} }
      },
      retryAtTimestamp: Date.now() + delayMs
    }
  }

  // Stop the log feed whatever state it's in — an open stream, an in-flight connect, or a
  // pending retry — and mark it closed. A clean stop reports 'cancelled' to `onParseEnded`,
  // which deliberately does not auto-restart. Used by scroll-pause (so the user can read back
  // through history with no "connection lost" banner) and by teardown when leaving the tab or
  // switching pipelines.
  const stopLogStream = (pipelineName: string) => {
    const stream = streams[pipelineName]?.stream
    if (!stream) {
      return
    }
    if ('open' in stream) {
      stream.stop()
      // Mark closed now rather than waiting for the (delayed) onParseEnded tick, so a
      // scroll-resume that arrives within the flush window sees a closed stream and reconnects.
      streams[pipelineName].stream = { closed: {} }
    } else if ('cancelFetch' in stream) {
      stream.cancelFetch()
    } else if ('cancelRetry' in stream) {
      stream.cancelRetry()
    }
  }
  // Scroll-resume: when the view sticks to the bottom again, reconnect the feed. Drop any
  // pending retry first so we connect immediately rather than waiting out the backoff.
  const resumeLogStream = (pipelineName: string) => {
    const s = streams[pipelineName]?.stream
    if (!s) {
      return
    }
    if ('cancelRetry' in s) {
      s.cancelRetry()
    }
    if ('open' in s || 'cancelFetch' in s) {
      return
    }
    startStream(pipelineName, 0)
  }
  const onStickToBottomChange = (stickToBottom: boolean) => {
    if (deleted) {
      return
    }
    if (stickToBottom) {
      resumeLogStream(pipelineName)
    } else {
      stopLogStream(pipelineName)
    }
  }

  $effect(() => {
    const interval = setInterval(() => {
      getStreams.current = streams
    }, 300)
    return () => clearInterval(interval)
  })
  $effect(() => {
    untrack(() => pipelineActionCallbacks.add('', 'delete', dropLogHistory))
    return () => {
      pipelineActionCallbacks.remove('', 'delete', dropLogHistory)
    }
  })
  let stream = $derived(pipelineLogs.stream)
  const now = useInterval(() => new Date(), 1000, 1000 - (Date.now() % 1000))
</script>

<div class="relative flex h-full flex-1 flex-col rounded">
  {#if deleted}
    {#if pipelineLogs.rows.length}
      <WarningBanner variant="info">
        Displaying cached log history. The pipeline has been deleted.
      </WarningBanner>
    {:else}
      <WarningBanner variant="info">
        There are no logs available. The pipeline has been deleted.
      </WarningBanner>
    {/if}
  {:else if 'closed' in stream}
    {#if 'retryAtTimestamp' in stream && pipelineStatusName !== 'Preparing' && pipelineStatusName !== 'Provisioning' && pipelineStatusName !== 'Initializing'}
      <WarningBanner>
        {@const seconds = Math.floor(
          Dayjs.duration(stream.retryAtTimestamp - now.current.valueOf()).asSeconds()
        )}
        Connection to logs stream lost.
        {#if seconds > 0}Retrying in
          {seconds}s...
        {:else}
          Retrying in 1s...
        {/if}
      </WarningBanner>
    {:else if !areLogsExpected(pipelineStatusName)}
      {#if pipelineLogs.rows.length}
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
  {:else if 'cancelFetch' in stream}
    <WarningBanner>Connecting to logs stream...</WarningBanner>
  {/if}
  {#key pipelineName}
    <LogsStreamList
      logs={pipelineLogs}
      search={logSearch}
      onSearchShortcut={onLogSearchShortcut}
      {onStickToBottomChange}
    ></LogsStreamList>
  {/key}
</div>
