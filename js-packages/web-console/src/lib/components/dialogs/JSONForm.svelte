<script lang="ts">
  import MonacoEditor from '$lib/components/MonacoEditorRunes.svelte'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  import { isMonacoEditorDisabled } from '$lib/functions/common/monacoEditor'
  import { useSkeletonTheme } from '$lib/compositions/useSkeletonTheme.svelte'
  import { useCodeEditorSettings } from '$lib/compositions/pipelines/useCodeEditorSettings.svelte'

  import * as MonacoImports from 'monaco-editor'
  import { editor } from 'monaco-editor/esm/vs/editor/editor.api'

  let {
    filePath,
    onSubmit,
    value = $bindable(),
    readOnlyMessage,
    disabled,
    refreshOnChange = true
  }: {
    filePath: string
    onSubmit: (json: string) => Promise<void>
    value?: string
    readOnlyMessage?: { value: string }
    disabled?: boolean
    refreshOnChange?: boolean
  } = $props()

  const theme = useSkeletonTheme()
  const darkMode = useDarkMode()
  const { editorFontSize } = useCodeEditorSettings()

  let currentModel: editor.ITextModel = $state(undefined!)
  let editorRef: editor.IStandaloneCodeEditor = $state()!

  const getValue = () => currentModel?.getValue() ?? ''

  const updateUpstream = () => {
    value = getValue()
  }

  const modelUri = $derived(MonacoImports.Uri.file(filePath))

  $effect(() => {
    if (!refreshOnChange) {
      return
    }
    value
    if (editor.getModel(modelUri)?.getValue() !== value) {
      if (value) {
        editor.getModel(modelUri)?.setValue(value)
      }
    }
  })

  $effect(() => {
    return () => {
      currentModel?.dispose()
      currentModel = undefined!
    }
  })

  $effect.pre(() => {
    const model = editor.getModel(modelUri) ?? editor.createModel(value ?? '', 'json', modelUri)
    currentModel = model
  })
</script>

<MonacoEditor
  model={currentModel}
  onready={(ref) => {
    ref.onKeyDown((e) => {
      if (e.code === 'KeyS' && (e.ctrlKey || e.metaKey)) {
        const currentValue = currentModel.getValue()
        updateUpstream()
        onSubmit(currentValue)
        e.preventDefault()
      }
    })

    // Update bound value when editor loses focus (avoiding keystroke updates)
    ref.onDidBlurEditorText(() => {
      setTimeout(() => updateUpstream())
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
