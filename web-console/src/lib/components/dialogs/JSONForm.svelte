<script lang="ts">
  import MonacoEditor from '$lib/components/MonacoEditorRunes.svelte'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  import { isMonacoEditorDisabled } from '$lib/functions/common/monacoEditor'
  import { useSkeletonTheme } from '$lib/compositions/useSkeletonTheme.svelte'
  import { useCodeEditorSettings } from '$lib/compositions/pipelines/useCodeEditorSettings.svelte'

  import * as MonacoImports from 'monaco-editor'
  import { editor } from 'monaco-editor/esm/vs/editor/editor.api'

  let {
    json,
    filePath,
    onSubmit,
    getData = $bindable(),
    readOnlyMessage,
    disabled
  }: {
    json: string
    filePath: string
    onSubmit: (json: string) => Promise<void>
    getData?: () => string
    readOnlyMessage?: { value: string }
    disabled?: boolean
  } = $props()
  const theme = useSkeletonTheme()
  const darkMode = useDarkMode()
  const { editorFontSize } = useCodeEditorSettings()
  getData = () => currentModel?.getValue() ?? ''

  let currentModel: editor.ITextModel = $state(undefined!)
  $effect.pre(() => {
    const modelUri = MonacoImports.Uri.file(filePath)
    const model = editor.getModel(modelUri) ?? editor.createModel(json, 'json', modelUri)
    currentModel = model
  })
  let editorRef: editor.IStandaloneCodeEditor = $state()!
</script>

<MonacoEditor
  model={currentModel}
  onready={(x) => {
    x.onKeyDown((e) => {
      if (e.code === 'KeyS' && (e.ctrlKey || e.metaKey)) {
        onSubmit(currentModel.getValue())
        e.preventDefault()
      }
    })
  }}
  bind:editor={editorRef}
  options={{
    fontFamily: theme.config.monospaceFontFamily,
    fontSize: editorFontSize.value,
    theme: darkMode.current === 'dark' ? 'feldera-dark' : 'feldera-light',
    automaticLayout: true,
    lineNumbersMinChars: 2,
    overviewRulerLanes: 0,
    hideCursorInOverviewRuler: true,
    overviewRulerBorder: false,
    scrollbar: {
      vertical: 'visible'
    },
    minimap: {
      enabled: false
    },
    stickyScroll: {
      enabled: false
    },
    scrollBeyondLastLine: false,
    readOnlyMessage,
    ...isMonacoEditorDisabled(disabled)
  }}
/>
