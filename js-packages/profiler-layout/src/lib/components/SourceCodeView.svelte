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
  import { untrack } from 'svelte'
  import type { SourceFile } from '../functions/processZipBundle'

  interface Props {
    /** The view reads `name` and `language` once, at mount. `name` is the URI of the Monaco
     *  model, so two mounted views must show different files. */
    file: SourceFile
    highlightRanges?: SourcePositionRange[]
  }

  let { file, highlightRanges = [] }: Props = $props()

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

  // The view updates its model when `file.code` changes, for example when a new bundle loads. It
  // disposes the model when it unmounts.
  const { name, language } = untrack(() => file)
  const modelUri = monaco.Uri.parse(`inmemory://profiler/${name}`)
  const model = monaco.editor.createModel('', language, modelUri)

  $effect(() => {
    if (model.getValue() !== file.code) {
      model.setValue(file.code)
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
      // The compiler's `end` is the last character of the range; `setSelections` converts it to the
      // caret position Monaco selects to, so pass the range through unchanged.
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
