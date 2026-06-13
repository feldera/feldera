<script lang="ts" module>
  import {
    FELDERA_DARK_THEME,
    FELDERA_LIGHT_THEME,
    registerFelderaMonacoThemes
  } from './sqlCodeTheme'

  // Define the themes once per module load — Monaco's `defineTheme` is idempotent, and the
  // values here match the pipeline editor's registration so co-mounting both views is safe.
  registerFelderaMonacoThemes()
</script>

<script lang="ts">
  import { MonacoEditor, setSelections } from 'common-ui'
  import * as monaco from 'monaco-editor'
  import type { SourcePositionRange } from 'profiler-lib'

  interface Props {
    code: string
    highlightRanges?: SourcePositionRange[]
  }

  let { code, highlightRanges = [] }: Props = $props()

  let editor: monaco.editor.IStandaloneCodeEditor | undefined = $state()
  let isDarkMode = $state(false)

  // Track the `dark` class on <html> (set by the host app's dark-mode toggle).
  $effect(() => {
    const read = () => (isDarkMode = document.documentElement.classList.contains('dark'))
    read()
    const observer = new MutationObserver(read)
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ['class'] })
    return () => observer.disconnect()
  })

  // This component is mounted once (hoisted into a <PersistentContent> overlay by
  // SupportBundleViewerLayout), so it owns a single model on a fixed URI. The model's contents are
  // refreshed whenever `code` changes — including when a new support bundle loads a different
  // program — and the model is disposed when the component unmounts.
  const modelUri = monaco.Uri.parse('inmemory://profiler/program.sql')
  const model = monaco.editor.createModel('', 'sql', modelUri)

  $effect(() => {
    if (model.getValue() !== code) {
      model.setValue(code)
    }
  })

  $effect(() => {
    return () => model.dispose()
  })

  $effect(() => {
    if (!editor) {
      return
    }
    setSelections(
      editor,
      highlightRanges.map((r) => ({ start: r.start, end: r.end }))
    )
  })
</script>

<MonacoEditor
  {model}
  bind:editor
  extras={{ isDarkMode }}
  options={{
    readOnly: true,
    theme: isDarkMode ? FELDERA_DARK_THEME : FELDERA_LIGHT_THEME,
    automaticLayout: true,
    lineNumbersMinChars: 3,
    minimap: { enabled: false },
    scrollBeyondLastLine: false,
    overviewRulerLanes: 0,
    hideCursorInOverviewRuler: true,
    overviewRulerBorder: false,
    folding: false
  }}
  onready={() => {}}
/>
