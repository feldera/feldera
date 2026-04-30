<script lang="ts">
  import { editor, Uri } from 'monaco-editor'
  import MonacoEditor from '$lib/components/MonacoEditorRunes.svelte'
  import { effectMonacoContentPlaceholder } from '$lib/components/monacoEditor/effectMonacoContentPlaceholder.svelte'
  import { useGlobalDialog } from '$lib/compositions/layout/useGlobalDialog.svelte'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import { useSkeletonTheme } from '$lib/compositions/useSkeletonTheme.svelte'
  import type { StatusName } from '$lib/services/pipelineManager'

  const MODEL_URI = 'connectors:/connectors.toml'
  const PLACEHOLDER = `# List Rust crates implementing Feldera connector plugins.\n# example = "1.0"\n# my_connector = { git = "https://github.com/acme/feldera-connector", tag = "v1.0.0" }`

  const api = usePipelineManager()
  const globalDialog = useGlobalDialog()
  const darkMode = useDarkMode()
  const theme = useSkeletonTheme()

  let etag: string | null = $state(null)
  let connectorStatus: StatusName | null = $state(null)
  let errorMessage: string | null = $state(null)
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

  const close = () => {
    globalDialog.dialog = null
  }

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
      } else if (result.status === 412) {
        errorMessage =
          'Another operator updated this file. Close and reopen to see the latest version.'
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

  $effect(() => {
    globalDialog.onClickAway = close
  })
</script>

<div class="flex h-[80vh] flex-col gap-4 p-4 sm:p-6" data-testid="box-plugins-dialog">
  <div class="flex flex-nowrap items-center justify-between">
    <div class="h5">Plugins</div>
    <button onclick={close} class="fd fd-x -m-2 btn-icon text-[24px]" aria-label="Close dialog"
    ></button>
  </div>

  <div class="flex flex-1 flex-col overflow-hidden rounded-container bg-surface-50-950 px-4 py-2">
    <div class="flex flex-nowrap items-center">
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

    <div class="h-4"></div>
  </div>

  {#if errorMessage}
    <div class="text-sm text-error-500" data-testid="box-plugins-error">{errorMessage}</div>
  {/if}

  <div class="flex flex-nowrap justify-end gap-4">
    <button
      onclick={close}
      class="btn preset-filled-surface-50-950 px-4"
      data-testid="btn-plugins-close"
    >
      Close
    </button>
    <button
      onclick={apply}
      disabled={applying}
      class="btn preset-filled-primary-500 px-4"
      data-testid="btn-plugins-apply"
    >
      {applying ? 'Applying…' : 'Apply'}
    </button>
  </div>
</div>
