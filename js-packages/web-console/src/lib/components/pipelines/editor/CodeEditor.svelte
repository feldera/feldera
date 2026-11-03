<script lang="ts" module>
  const openFiles: Record<
    string,
    {
      sync: DecoupledStateProxy<string>
      model: editor.ITextModel
      view: editor.ICodeEditorViewState | null
    }
  > = {}

  /**
   * Dispose a file's resources by path. Safe to call when the file is not open —
   * no-op in that case. Also safe to call from a component's cleanup after the
   * editor instance is gone, since it only touches the module-level map.
   */
  export function disposeFile(filePath: string) {
    if (!openFiles[filePath]) {
      return
    }
    openFiles[filePath].sync[Symbol.dispose]()
    openFiles[filePath].model.dispose()
    delete openFiles[filePath]
  }

  MonacoImports.editor.defineTheme('feldera-light', {
    base: 'vs',
    inherit: true,
    rules: [{ token: 'string.sql', foreground: '#7a3d00' }],
    colors: {
      // White, like the panel it sits in, rather than the tinted page behind the panel.
      'editor.background': '#ffffff',
      'editor.inactiveSelectionBackground': '#add6ff90' // More visible selection when editor is not focused
    }
  })

  MonacoImports.editor.defineTheme('feldera-dark', {
    base: 'vs-dark',
    inherit: true,
    rules: [{ token: 'string.sql', foreground: '#d9731a' }],
    colors: {
      'editor.background': getThemeColor('--body-background-color-dark').format('hex'),
      'editor.inactiveSelectionBackground': '#264f7890' // More visible selection when editor is not focused
    }
  })
</script>

<script lang="ts">
  import { untrack } from 'svelte'
  import { DecoupledStateProxy } from '#lib/compositions/decoupledState.svelte.ts'
  import { useDarkMode } from '#lib/compositions/useDarkMode.svelte.ts'
  import { isMonacoEditorDisabled } from '#lib/functions/monacoEditor.ts'
  import { getThemeColor, MonacoEditor, setSelections as setEditorSelections } from 'common-ui'
  import * as MonacoImports from 'monaco-editor'
  import { editor, KeyCode, KeyMod } from 'monaco-editor'
  import type { EditorLanguage } from 'monaco-editor/esm/metadata.js'
  import { useSkeletonTheme } from '#lib/compositions/useSkeletonTheme.svelte.ts'
  import { effectMonacoContentPlaceholder } from '#lib/components/monacoEditor/effectMonacoContentPlaceholder.svelte.ts'
  import { GenericOverlayWidget } from '#lib/components/monacoEditor/GenericOverlayWidget.ts'
  import { useCodeEditorSettings } from '#lib/compositions/pipelines/useCodeEditorSettings.svelte.ts'
  import { felderaCompilerMarkerSource } from '#lib/functions/pipelines/monaco.ts'
  import type { Snippet } from '#lib/types/svelte.ts'

  void MonacoImports // Explicitly import all monaco-editor esm modules

  let {
    path,
    files,
    currentFileName = $bindable(),
    editDisabled,
    readOnlyMessage,
    codeEditor,
    statusBarCenter,
    statusBarEnd,
    toolBarStart,
    toolBarEnd,
    fileTab,
    beforeTextArea,
    autoSave,
    confirmSave = async () => true,
    downstreamChanged: _downstreamChanged = $bindable(),
    saveFile: _saveFile = $bindable(async () => false),
    isFocused: _isFocused = $bindable(false)
  }: {
    path: string
    files: {
      name: string
      access: { current: string }
      language?: EditorLanguage
      markers?: Record<string, editor.IMarkerData[]>
      behaviorOnConflict?: 'auto-pull' | 'auto-push' | 'promt'
      placeholder?: string
      // Why a file without a setter is read-only; defaults to the compiler-generated hint.
      readOnlyMessage?: string
      // Unsaved content to load over the saved content when the editor opens the file.
      initialDraft?: string
    }[]
    currentFileName: string
    editDisabled?: boolean
    // Overrides the generic read-only hint for a writable file, e.g. to explain
    // a permission-driven read-only state. Ignored for compiler-generated files.
    readOnlyMessage?: string
    codeEditor: Snippet<[textEditor: Snippet, statusBar: Snippet, isReadOnly: boolean]>
    statusBarCenter?: Snippet
    statusBarEnd?: Snippet<[downstreamChanged: boolean]>
    /** Rendered before the file tabs, at the start of the tab row. */
    toolBarStart?: Snippet
    toolBarEnd?: Snippet<[{ saveFile: () => void }]>
    fileTab: Snippet<[text: string, onclick: () => void, isCurrent: boolean, isSaved: boolean]>
    beforeTextArea?: Snippet
    /** Whether edits save on their own. Defaults to the user's Autosave setting. */
    autoSave?: boolean
    /**
     * Called before every save except the debounced autosave, which saves without it.
     * Resolve to `false` to cancel the save. Turn `autoSave` off while saves need a
     * confirmation.
     */
    confirmSave?: () => Promise<boolean>
    downstreamChanged?: boolean
    /** Saves all unsaved files. Resolves to `false` if `confirmSave` cancels the save. */
    saveFile?: () => Promise<boolean>
    isFocused?: boolean
  } = $props()

  /**
   * Close a file by disposing its resources. Forwards to the module-level
   * `disposeFile` for callers that only have a `bind:this` handle.
   * @param filePath - The full path to the file (e.g., "pipelineName/program.sql")
   */
  export function closeFile(filePath: string) {
    disposeFile(filePath)
  }

  type CodePosition = { line: number; column: number }

  export function openFile(fileName: string) {
    if (!editorRef) {
      return
    }
    if (currentFileName !== fileName) {
      currentFileName = fileName
    }
  }

  /**
   * Reveal a specific line and column of the current file in the editor viewport.
   * @param fileName - The name of the file (e.g., "program.sql")
   * @param line - The line number to reveal
   * @param column - Optional column number (defaults to 1)
   */
  export function revealLine(/*fileName: string,*/ pos: CodePosition) {
    if (!editorRef) {
      return
    }
    setTimeout(() => {
      editorRef.revealPosition({ lineNumber: pos.line, column: pos.column ?? 1 })
    }, 50)
  }

  export function setSelections(ranges: { start: CodePosition; end: CodePosition }[]) {
    setEditorSelections(editorRef, ranges)
  }

  let editorRef: editor.IStandaloneCodeEditor = $state()!
  const { editorFontSize, autoSaveFiles, showMinimap, showStickyScroll } = useCodeEditorSettings()

  let isFocused = $state(false)

  $effect(() => {
    if (!editorRef) return

    const disposables = [
      editorRef.onDidFocusEditorText(() => {
        isFocused = true
      }),
      editorRef.onDidBlurEditorText(() => {
        isFocused = false
      })
    ]

    return () => {
      disposables.forEach((d) => d.dispose())
    }
  })

  $effect(() => {
    _isFocused = isFocused
  })

  let isAutoSaveOn = $derived(autoSave ?? autoSaveFiles.value)
  let wait = $derived(isAutoSaveOn ? 2000 : ('decoupled' as const))
  let file = $derived(files.find((f) => f.name === currentFileName)!)

  function isReadonlyProperty<T>(obj: T, prop: keyof T) {
    return !Object.getOwnPropertyDescriptor(obj, prop)?.['set']
  }
  const isReadOnlyFile = $derived(isReadonlyProperty(file.access, 'current'))
  let isReadOnly = $derived(editDisabled || isReadOnlyFile)

  const getFilePath = (file: { name: string }) => path + '/' + file.name
  let filePath = $derived(getFilePath(file))
  let previousFilePath = $state<string | undefined>(undefined)

  const initFileState = (file: (typeof files)[number]) => {
    const filePath = getFilePath(file)
    if (openFiles[filePath]) {
      return
    }
    const modelUri = MonacoImports.Uri.file(filePath)
    const model = (() => {
      return (
        editor.getModel(modelUri) ??
        editor.createModel(file.access.current, file.language, modelUri)
      )
    })()
    const sync = new DecoupledStateProxy(
      file.access,
      {
        get current() {
          return model.getValue()
        },
        set current(v: string) {
          model.setValue(v)
        }
      },
      () => wait
    )
    model.onDidChangeContent((e) => {
      sync.touch()
    })
    openFiles[filePath] = {
      sync,
      model,
      view: null
    }
    if (file.initialDraft) {
      // Fires onDidChangeContent, so the file shows as unsaved.
      model.setValue(file.initialDraft)
    }
  }

  // Open every file with a draft right away, so that its tab shows as unsaved and
  // `saveAll` includes it.
  $effect.pre(() => {
    initFileState(file)
    files.filter((file) => file.initialDraft).forEach(initFileState)
  })

  /**
   * Save every unsaved file if `confirmSave` allows it. Every save except the
   * debounced autosave calls this function.
   * @returns `false` if `confirmSave` cancels the save.
   */
  const saveAll = async () => {
    const unsaved = files
      .map((file) => openFiles[getFilePath(file)]?.sync)
      .filter((sync) => sync && (sync.downstreamChanged || sync.upstreamChanged))
    if (unsaved.length === 0) {
      return true
    }
    if (!(await confirmSave())) {
      return false
    }
    unsaved.forEach((sync) => sync.push())
    return true
  }

  $effect.pre(() => {
    file.access.current
    untrack(() => {
      openFiles[filePath].sync.fetch(file.access)
    })
  })

  $effect(() => {
    if (!openFiles[filePath].sync.upstreamChanged) {
      return
    }
    if (file.behaviorOnConflict === 'promt' || file.behaviorOnConflict === undefined) {
      if (!openFiles[filePath].sync.downstreamChanged) {
        openFiles[filePath].sync.pull()
        return
      }
      const widget = new GenericOverlayWidget(editorRef, conflictWidgetRef, {
        id: 'editor.widget.upstreamUpdateConflict',
        position: editor.OverlayWidgetPositionPreference.BOTTOM_RIGHT_CORNER
      })
      return () => {
        widget.dispose()
      }
    }
    if (file.behaviorOnConflict === 'auto-pull') {
      openFiles[filePath].sync.pull()
      return
    }
    if (file.behaviorOnConflict === 'auto-push') {
      void saveAll()
      return
    }
  })
  let currentModel: editor.ITextModel = $state(undefined!)
  $effect.pre(() => {
    currentModel = openFiles[filePath].model
  })
  $effect.pre(() => {
    filePath
    $effect.root(() => {
      if (!editorRef) {
        return
      }
      // Save last file's scroll position
      if (previousFilePath) {
        openFiles[previousFilePath].view = editorRef.saveViewState()!
      }
      previousFilePath = filePath
      // Restore current file's scroll position
      setTimeout(() => {
        editorRef.restoreViewState(openFiles[filePath].view)
      }, 1)
    })
  })

  $effect(() => {
    // Trigger save right away when autosave is turned on
    if (!isAutoSaveOn) {
      return
    }
    setTimeout(() => void saveAll())
  })

  let placeholderContent = $derived(file.placeholder)
  $effect(() => {
    return effectMonacoContentPlaceholder(editorRef, placeholderContent, { opacity: '70%' })
  })

  $effect(() => {
    _downstreamChanged = files.some(
      (file) => openFiles[getFilePath(file)]?.sync.downstreamChanged ?? false
    )
  })

  $effect(() => {
    _saveFile = saveAll
  })

  let conflictWidgetRef: HTMLElement = $state(undefined!)
  const darkMode = useDarkMode()
  const theme = useSkeletonTheme()
</script>

<div class="hidden" bind:this={conflictWidgetRef}>
  <div class="relative flex flex-col gap-4 bg-surface-50-950 p-4">
    <div>
      <span class="fd fd-triangle-alert text-[20px] text-warning-500"> </span>
      The pipeline code was changed outside this window since you started editing.<br />
      Please resolve the conflict to save your changes.
    </div>
    <div class="flex flex-nowrap justify-end gap-4">
      <button
        class=" !rounded-0 bg-surface-100-900 px-2 py-1 hover:preset-outlined-primary-500"
        onclick={() => openFiles[filePath].sync.ignoreUpstream()}
      >
        Keep Editing
      </button>
      <button
        class=" !rounded-0 bg-surface-100-900 px-2 py-1 hover:preset-outlined-primary-500"
        onclick={() => openFiles[filePath].sync.pull()}
      >
        Accept Remote
      </button>
      <button
        class=" !rounded-0 bg-surface-100-900 px-2 py-1 hover:preset-outlined-primary-500"
        onclick={() => void saveAll()}
      >
        Accept Local
      </button>
    </div>
  </div>
</div>

{@render codeEditor(textEditor, statusBar, isReadOnly)}
{#snippet textEditor()}
  <div class="flex h-full flex-col">
    <!-- A grey 40px tab bar: the file tabs are cells divided by 1px lines, and the open one
         is white and covers the bar's bottom line, so it joins the editor below. -->
    <div class="flex flex-row-reverse flex-wrap items-center border-b border-line bg-toolbar">
      {@render toolBarEnd?.({ saveFile: _saveFile })}
      <div class="mr-auto flex flex-nowrap items-start self-start">
        {@render toolBarStart?.()}
        {#each files as file}
          {@render fileTab(
            file.name,
            () => (currentFileName = file.name),
            file.name === currentFileName,
            !(openFiles[getFilePath(file)]?.sync.downstreamChanged ?? false)
          )}
        {/each}
      </div>
    </div>
    {@render beforeTextArea?.()}
    <div class="relative flex-1">
      <div
        class="absolute h-full w-full"
        onkeydown={(e) => {
          if (e.code === 'Period' && e.key === ':') {
            // Workaround for Firefox browser when using AZERTY layout
            editorRef.trigger('Firefox AZERTY workaround', 'editor.action.commentLine', undefined)
          }
        }}
      >
        <MonacoEditor
          markers={file.markers}
          markerSource={felderaCompilerMarkerSource}
          onready={(editorRef) => {
            editorRef.addCommand(KeyMod.CtrlCmd | KeyCode.KeyS, () => {
              void saveAll()
            })
            editorRef.addCommand(KeyMod.CtrlCmd | KeyCode.KeyM, () => {
              const minimapOptions = editorRef.getOption(editor.EditorOption.minimap)
              showMinimap.value = !minimapOptions.enabled
            })
            editorRef.addCommand(KeyMod.CtrlCmd | KeyCode.KeyK, () => {
              const stickyScrollOptions = editorRef.getOption(editor.EditorOption.stickyScroll)
              showStickyScroll.value = !stickyScrollOptions.enabled
            })
          }}
          bind:editor={editorRef}
          model={currentModel}
          extras={{ isDarkMode: darkMode.current === 'dark' }}
          options={{
            readOnlyMessage: {
              value: isReadOnlyFile
                ? (file.readOnlyMessage ?? 'Cannot edit a compiler-generated file')
                : (readOnlyMessage ??
                  'Cannot edit code while the pipeline is running or its storage is in use')
            },
            fontFamily: theme.config.monospaceFontFamily,
            fontSize: editorFontSize.value,
            theme: ['feldera-dark', 'feldera-light'][+(darkMode.current === 'light')],
            automaticLayout: true,
            lineNumbersMinChars: 3,
            ...isMonacoEditorDisabled(isReadOnly),
            renderValidationDecorations: 'on', // Show red error squiggles even in read-only mode
            overviewRulerLanes: 0,
            hideCursorInOverviewRuler: true,
            overviewRulerBorder: false,
            scrollbar: {
              vertical: 'visible'
            },
            minimap: {
              enabled: showMinimap.value
            },
            stickyScroll: {
              enabled: showStickyScroll.value
            }
          }}
        />
      </div>
    </div>
  </div>
{/snippet}

{#snippet statusBar()}
  <div class="flex h-6 flex-nowrap gap-3">
    {@render statusBarCenter?.()}
  </div>
  <div class="ml-auto flex flex-nowrap gap-x-2">
    {@render statusBarEnd?.(openFiles[filePath].sync.downstreamChanged)}
  </div>
{/snippet}
