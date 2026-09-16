<script lang="ts" module>
  import {
    formatLogCursor,
    isExactResume,
    type LogCursor,
    type LogResume,
    parseLogResume
  } from '$lib/functions/pipelines/logCursor'

  const streams: Record<
    string,
    {
      firstRowIndex: number
      rows: string[]
      totalSkippedBytes: number
      /** Lines the server discarded before the oldest one on screen, as of the last catch-up. */
      totalDiscardedLines: number
      /**
       * Why the last attempt to connect failed, or null once one succeeds. Held beside the
       * rows rather than pushed into them: a connection error is not a log line, and an
       * exact resume keeps the rows, so pushing would strand one notice per failed attempt
       * in the middle of the log.
       */
      connectionError: string | null
      /**
       * How far we have read. Sent back on the next connection so the server can carry
       * on from there. Null if the server does not support resuming, or if we can no
       * longer trust our line count; either way the next connection starts over.
       */
      cursor: LogCursor | null
      stream:
        | { cancelFetch: () => void }
        | { open: ReadableStream<Uint8Array>; stop: () => void }
        | { closed: {} }
        | { closed: {}; cancelRetry: () => void; retryAtTimestamp: number }
    }
  > = {}

  /**
   * Handles the first batch of a new connection: now that we know where the server picked
   * us up, decides whether to keep the lines already on screen or clear them out.
   *
   * Held back until the first batch rather than done the moment the position arrives, so a
   * reconnect does not blank the view while it waits for the first byte.
   */
  const openConnection = (
    pipelineName: string,
    requested: LogCursor | null,
    resumed: LogResume | null
  ) => {
    const stream = streams[pipelineName]
    if (!isExactResume(requested, resumed)) {
      stream.rows = []
      stream.firstRowIndex = 0
      stream.totalSkippedBytes = 0
      // Every line before the first one about to arrive is a line we cannot show. `gap`
      // counts only from the cursor we asked for, which leaves out what the server had
      // already discarded before that. A server that reports nothing leaves us guessing,
      // and it prints its own notice as a log line.
      stream.totalDiscardedLines = resumed?.seq ?? 0
    }
    stream.cursor = resumed ? { epoch: resumed.epoch, seq: resumed.seq } : null
  }

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
    onLogMatchCountChange
  }: {
    pipeline: { current: ExtendedPipeline }
    deleted?: boolean
    /** Submitted search state, owned by the monitoring panel (which hosts the search bar in
     *  its toolbar). */
    logSearch?: SearchState
    /** Reports the current match count so the panel can drive its search counter/nav buttons. */
    onLogMatchCountChange?: (count: number) => void
  } = $props()
  let pipelineName = $derived(pipeline.current.name)

  let pipelineStatusName = $derived(unionName(pipeline.current.status))

  let pipelineLogs = $derived.by(() => {
    if (!streams[pipelineName]) {
      streams[pipelineName] = {
        firstRowIndex: 0,
        stream: { closed: {} },
        rows: [],
        totalSkippedBytes: 0,
        totalDiscardedLines: 0,
        connectionError: null,
        cursor: null
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
        'ConcurrentBootstrapping',
        'Synchronizing',
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
    // Identifies this connection before there is an open stream to identify it by.
    const connection = {
      cancelFetch: () => {
        abortController.abort()
        streams[pipelineName].stream = { closed: {} }
      }
    }
    streams[pipelineName].stream = connection
    // Ask the server to carry on from where we stopped, so reconnecting only costs us the
    // lines we missed rather than the whole log again. An empty cursor still asks to be
    // told our position; leaving the parameter out altogether selects the old behaviour.
    const requestedCursor = streams[pipelineName].cursor
    api
      .pipelineLogsStream(pipelineName, formatLogCursor(requestedCursor), {
        signal: abortController.signal
      })
      .then((result) => {
        // Ignore the response if this is no longer the current connection. It may have been
        // cancelled, or replaced by a newer one, or the log history may have been dropped.
        // Without this check two connections would write to the same rows and cursor.
        if (streams[pipelineName]?.stream !== connection) {
          return
        }
        if (result instanceof Error) {
          streams[pipelineName].stream = { closed: {} }
          streams[pipelineName].connectionError = result.message
          // `cause` is the API's error body, with the status added by `streamingFetch`.
          const cause = result.cause as { error_code?: string; status?: number } | undefined
          // A cursor the server rejects will not become acceptable by being sent again, so
          // keeping it would leave the viewer looping over the same error and never showing
          // another log line. Drop it and let the next attempt start the log over.
          if (cause?.error_code === 'InvalidLogCursorParam') {
            streams[pipelineName].cursor = null
          }
          // 503 is the server asking for a slower retry, so each one widens the backoff.
          tryRestartStream(pipelineName, cause?.status === 503 ? attempts + 1 : 0)
          return
        }
        // Where the server picked us up. It arrives with the response headers, so we hold
        // it until the first batch: touching the rows already on screen the moment the
        // fetch resolves would leave a reconnect (scroll-resume, retry) blank between
        // connecting and the first byte. Until then the old rows stay put.
        const resumed = parseLogResume(result.response.headers)
        let freshConnection = true
        // Lines the decoder dropped since the last batch. We hold the count and add it
        // when that batch arrives, rather than counting it straight away. If the connection
        // dies in between, none of those lines reached us, and a cursor that had already
        // counted them would skip past real lines on the next connection.
        let pendingSkippedLines = 0
        const { cancel } = parseStream<string>(
          result,
          newlineTextDecoder({
            bufferSize: 16 * 1024 * 1024,
            onSkipped: ({ bytes, lines }) => {
              streams[pipelineName].totalSkippedBytes += bytes
              pendingSkippedLines += lines
            }
          }),
          {
            pushChanges: (changes: string[]) => {
              if (freshConnection) {
                freshConnection = false
                openConnection(pipelineName, requestedCursor, resumed)
              }
              const droppedNum = pushAsCircularBuffer(
                () => streams[pipelineName].rows,
                bufferSize,
                (v: string) => v
              )(changes)
              streams[pipelineName].firstRowIndex += droppedNum
              // Count every line the server sent, including the ones we dropped to keep
              // up. Otherwise reconnecting would fetch that stretch all over again, when
              // we had already chosen not to display it.
              if (streams[pipelineName].cursor) {
                streams[pipelineName].cursor.seq += changes.length + pendingSkippedLines
              }
              pendingSkippedLines = 0
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
        // Clearing the error here rather than on the first batch keeps it from outliving the
        // failure on a stream that is caught up and so has no line to send.
        streams[pipelineName].connectionError = null
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
        {#if pipelineLogs.connectionError}
          <span class="block whitespace-pre-line">{pipelineLogs.connectionError}</span>
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
      onMatchCountChange={onLogMatchCountChange}
      {onStickToBottomChange}
    ></LogsStreamList>
  {/key}
</div>
