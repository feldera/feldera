<script lang="ts">
  import { editor, Uri } from 'monaco-editor'
  import { Pane, PaneGroup, PaneResizer } from 'paneforge'
  import { untrack } from 'svelte'
  import AppHeader from '$lib/components/layout/AppHeader.svelte'
  import NavigationExtras from '$lib/components/layout/NavigationExtras.svelte'
  import PipelineBreadcrumbs from '$lib/components/layout/PipelineBreadcrumbs.svelte'
  import MonacoEditor from '$lib/components/MonacoEditorRunes.svelte'
  import { effectMonacoContentPlaceholder } from '$lib/components/monacoEditor/effectMonacoContentPlaceholder.svelte'
  import LogsStreamList from '$lib/components/pipelines/editor/LogsStreamList.svelte'
  import PipelineBanner from '$lib/components/pipelines/editor/PipelineBanner.svelte'
  import { useAdaptiveDrawer } from '$lib/compositions/layout/useAdaptiveDrawer.svelte'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import { useSkeletonTheme } from '$lib/compositions/useSkeletonTheme.svelte'
  import {
    parseCancellable,
    pushAsCircularBuffer,
    SplitNewlineTransformStream
  } from '$lib/functions/pipelines/changeStream'
  import { resolve } from '$lib/functions/svelte'
  import { connectorsBuildLogStream, type StatusName } from '$lib/services/pipelineManager'

  const MODEL_URI = 'connectors:/connectors.toml'
  const PLACEHOLDER = `# List Rust crates implementing Feldera connector plugins.
# example = "1.0"
# my_connector = { git = "https://github.com/acme/feldera-connector", tag = "v1.0.0" }`
  const LOG_BUFFER_SIZE = 10000

  const api = usePipelineManager()
  const darkMode = useDarkMode()
  const theme = useSkeletonTheme()
  const drawer = useAdaptiveDrawer('right')

  const breadcrumbs = $derived([
    ...(drawer.isMobileDrawer
      ? []
      : [
          {
            text: 'Home',
            href: resolve(`/`)
          }
        ]),
    {
      text: 'Plugins',
      href: resolve(`/plugins/`)
    }
  ])

  // ── Editor state ─────────────────────────────────────────────────────
  let etag: string | null = $state(null)
  let connectorStatus: StatusName | null = $state(null)
  let errorMessage: string | null = $state(null)
  let buildError: string | null = $state(null)
  let applying = $state(false)
  let loading = $state(true)

  let model: editor.ITextModel | null = $state(null)
  let editorRef: editor.IStandaloneCodeEditor | undefined = $state()

  const getOrCreateModel = (content: string): editor.ITextModel => {
    const uri = Uri.parse(MODEL_URI)
    const existing = editor.getModel(uri)
    if (existing) {
      existing.setValue(content)
      return existing
    }
    return editor.createModel(content, 'graphql', uri)
  }

  const loadContent = async () => {
    loading = true
    try {
      const { content, etag: e } = await api.getConnectorsToml()
      etag = e
      model = getOrCreateModel(content)
    } catch {
      etag = null
      model = getOrCreateModel('')
    } finally {
      loading = false
    }
  }

  const pollStatus = async () => {
    try {
      const statusData = await api.getConnectorsStatus()
      connectorStatus = statusData.state
      buildError = statusData.state === 'failed' ? (statusData.error ?? 'Build failed') : null
    } catch {}
  }

  $effect(() => {
    loadContent()
    pollStatus()
    const id = setInterval(pollStatus, 3000)
    return () => clearInterval(id)
  })

  $effect(() => {
    if (!editorRef) {
      return
    }
    return effectMonacoContentPlaceholder(editorRef, PLACEHOLDER, { opacity: '70%' })
  })

  const apply = async () => {
    if (applying || !model) {
      return
    }
    applying = true
    errorMessage = null
    try {
      const content = model.getValue()
      const result = await api.putConnectorsToml(content, etag ?? '')
      if (result.ok) {
        etag = result.content_hash
        // Trigger an immediate status poll so the dot transitions to
        // yellow without waiting for the 3s tick.
        pollStatus()
        // Reset the log panel for the new build.
        resetLogStream()
        startLogStream()
      } else if (result.status === 412) {
        errorMessage =
          'Another operator updated this file. Reload the page to see the latest version.'
      } else if (result.status === 400) {
        errorMessage = result.message
      }
    } catch (e) {
      errorMessage = e instanceof Error ? e.message : 'Failed to save'
    } finally {
      applying = false
    }
  }

  const dotClass = $derived(
    connectorStatus === 'ready'
      ? 'bg-success-500'
      : connectorStatus === 'building'
        ? 'bg-warning-500'
        : 'bg-error-500'
  )

  // ── Build-log stream ─────────────────────────────────────────────────
  //
  // Mirrors the TabLogs.svelte composition pattern: streaming HTTP
  // response → SplitNewlineTransformStream → pushAsCircularBuffer →
  // <LogsStreamList />.
  //
  // The endpoint may return 404 on older servers, an empty body when
  // no build is active, or close cleanly when the current build's
  // session is replaced.  In all of those cases we want to silently
  // wait and reconnect rather than show a toast — the build-log panel
  // is best-effort live tail, not a critical request.
  let buildLogs: { rows: string[]; firstRowIndex: number; totalSkippedBytes: number } = $state({
    rows: [],
    firstRowIndex: 0,
    totalSkippedBytes: 0
  })
  let abortController: AbortController | null = null
  let cancelParse: (() => void) | null = null
  let reconnectTimer: ReturnType<typeof setTimeout> | null = null
  let unmounted = false
  const RECONNECT_DELAY_MS = 5_000

  const stopLogStream = () => {
    if (reconnectTimer) {
      clearTimeout(reconnectTimer)
      reconnectTimer = null
    }
    if (cancelParse) {
      cancelParse()
      cancelParse = null
    }
    if (abortController) {
      abortController.abort()
      abortController = null
    }
  }

  const resetLogStream = () => {
    stopLogStream()
    buildLogs = { rows: [], firstRowIndex: 0, totalSkippedBytes: 0 }
  }

  const scheduleReconnect = () => {
    if (unmounted || reconnectTimer) {
      return
    }
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null
      startLogStream()
    }, RECONNECT_DELAY_MS)
  }

  const startLogStream = async () => {
    if (abortController || unmounted) {
      return
    }
    const ac = new AbortController()
    abortController = ac
    const result = await connectorsBuildLogStream({ signal: ac.signal })
    if (ac.signal.aborted || unmounted) {
      abortController = null
      return
    }
    if (result instanceof Error) {
      // No endpoint, transient server error, or empty 404. Silently
      // back off and try again — the user does not need a toast for
      // an optional UX feature.
      abortController = null
      scheduleReconnect()
      return
    }
    const { cancel } = parseCancellable(
      result,
      {
        pushChanges: (changes: string[]) => {
          const droppedNum = pushAsCircularBuffer(
            () => buildLogs.rows,
            LOG_BUFFER_SIZE,
            (v: string) => v
          )(changes)
          buildLogs = {
            ...buildLogs,
            firstRowIndex: buildLogs.firstRowIndex + droppedNum
          }
        },
        onParseEnded: () => {
          // Stream end is the normal terminator when the bus' session
          // is replaced or the server closes the connection.  Tear
          // down our refs and reconnect after a short delay so the
          // next build's output picks up automatically.
          cancelParse = null
          abortController = null
          scheduleReconnect()
        },
        onBytesSkipped: (bytes) => {
          buildLogs = {
            ...buildLogs,
            totalSkippedBytes: buildLogs.totalSkippedBytes + bytes
          }
        }
      },
      new SplitNewlineTransformStream(),
      { bufferSize: 16 * 1024 * 1024 }
    )
    cancelParse = cancel
  }

  $effect(() => {
    untrack(() => startLogStream())
    return () => {
      unmounted = true
      stopLogStream()
    }
  })
</script>

<div class="flex h-full w-full flex-col" data-testid="box-plugins-page">
  <AppHeader>
    {#snippet afterStart()}
      <PipelineBreadcrumbs {breadcrumbs}></PipelineBreadcrumbs>
    {/snippet}
    {#snippet beforeEnd()}
      <NavigationExtras></NavigationExtras>
    {/snippet}
  </AppHeader>

  <div class="flex min-h-0 flex-1 flex-col gap-4 p-4 sm:p-6 pt-0 sm:pt-0">
    {#if errorMessage || buildError}
      <div data-testid="box-plugins-error">
        <PipelineBanner
          style="error"
          header={errorMessage ? 'Failed to apply' : 'Build failed'}
          message={errorMessage ?? buildError ?? undefined}
          onClose={errorMessage ? () => (errorMessage = null) : undefined}
        />
      </div>
    {/if}
    <PaneGroup direction="vertical" class="!overflow-visible">
      <!-- Top pane: connectors.toml editor with tab + Apply button. -->
      <Pane defaultSize={60} minSize={20} class="!overflow-visible">
        <div class="flex h-full flex-col overflow-hidden rounded-container bg-surface-50-950 p-2">
          <!-- Tab row: status-dotted tab on the left, Apply on the right. -->
          <div
            class="flex flex-nowrap items-center justify-between border-b border-surface-200-800"
          >
            <button
              class="flex flex-nowrap items-center gap-2 border-b-2 border-surface-950-50 py-2 pr-5 pl-2 font-medium sm:pl-3"
            >
              <div
                class="h-2.5 w-2.5 rounded-full {dotClass}"
                data-testid="dot-connectors-status"
                data-status={connectorStatus ?? 'not_configured'}
              ></div>
              connectors.toml
            </button>
            <div class="flex items-center gap-2 pr-2">
              <button
                onclick={apply}
                disabled={applying || loading}
                class="btn preset-filled-primary-500 px-4"
                data-testid="btn-plugins-apply"
              >
                {applying ? 'Applying…' : 'Apply'}
              </button>
            </div>
          </div>

          <div class="relative flex-1">
            {#if model}
              <div class="absolute h-full w-full">
                <MonacoEditor
                  bind:editor={editorRef}
                  {model}
                  onready={(e) => {
                    editorRef = e
                  }}
                  options={{
                    fontFamily: theme.config.monospaceFontFamily,
                    theme: ['feldera-dark', 'feldera-light'][+(darkMode.current === 'light')],
                    automaticLayout: true,
                    lineNumbersMinChars: 3,
                    overviewRulerLanes: 0,
                    hideCursorInOverviewRuler: true,
                    overviewRulerBorder: false,
                    scrollbar: { vertical: 'visible' },
                    minimap: { enabled: false }
                  }}
                />
              </div>
            {:else if loading}
              <div class="flex h-full items-center justify-center text-surface-500">Loading…</div>
            {/if}
          </div>
        </div>
      </Pane>

      <PaneResizer class="pane-divider-horizontal my-2" />

      <!-- Bottom pane: live build log. -->
      <Pane minSize={15}>
        <div
          class="flex h-full flex-col overflow-hidden rounded-container bg-surface-50-950 p-2"
          data-testid="box-plugins-build-log"
        >
          <div class="border-b border-surface-200-800 py-2 pr-5 pl-3 font-medium">Build log</div>
          <div class="relative flex-1 overflow-hidden">
            <LogsStreamList logs={buildLogs}></LogsStreamList>
          </div>
        </div>
      </Pane>
    </PaneGroup>
  </div>
</div>
