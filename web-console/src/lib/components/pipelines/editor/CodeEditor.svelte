<script lang="ts" module>
  let openFiles: Record<
    string,
    {
      sync: DecoupledStateProxy<string>
      model: editor.ITextModel
      view: editor.ICodeEditorViewState | null
    }
  > = {}

  MonacoImports.editor.defineTheme('feldera-light', {
    base: 'vs',
    inherit: true,
    rules: [{ token: 'string.sql', foreground: '#7a3d00' }],
    colors: {
      'editor.background': rgbToHex(
        getComputedStyle(document.body).getPropertyValue('--body-background-color').trim()
      )
    }
  })

  MonacoImports.editor.defineTheme('feldera-dark', {
    base: 'vs-dark',
    inherit: true,
    rules: [{ token: 'string.sql', foreground: '#d9731a' }],
    colors: {
      'editor.background': rgbToHex(
        getComputedStyle(document.body).getPropertyValue('--body-background-color-dark').trim()
      )
    }
  })

  MonacoImports.editor.defineTheme('feldera-light-disabled', {
    base: 'vs',
    inherit: true,
    rules: [{ token: 'string.sql', foreground: '#7a3d00' }],
    colors: {
      'editor.background': rgbToHex(
        getComputedStyle(document.body).getPropertyValue('--color-surface-50').trim()
      )
    }
  })

  MonacoImports.editor.defineTheme('feldera-dark-disabled', {
    base: 'vs-dark',
    inherit: true,
    rules: [{ token: 'string.sql', foreground: '#d9731a' }],
    colors: {
      'editor.background': rgbToHex(
        getComputedStyle(document.body).getPropertyValue('--color-surface-950').trim()
      )
    }
  })

  const pipelineActionCallbacks = usePipelineActionCallbacks()
  const dropOpenedFile = async (pipelineName: string) => {
    const files = ['program.sql', 'stubs.rs', 'udf.rs', 'udf.toml'].map(
      (file) => `${pipelineName}/${file}`
    )
    for (const file of files) {
      if (!openFiles[file]) {
        continue
      }
      openFiles[file].sync[Symbol.dispose]()
      openFiles[file].model.dispose()
      delete openFiles[file]
    }
  }
</script>

<script lang="ts">
  import { untrack, type Snippet } from 'svelte'
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'
  import { DecoupledStateProxy } from '$lib/compositions/decoupledState.svelte'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  import MonacoEditor, { isMonacoEditorDisabled } from '$lib/functions/common/monacoEditor'
  import * as MonacoImports from 'monaco-editor'
  import { editor, KeyCode, KeyMod } from 'monaco-editor/esm/vs/editor/editor.api'
  import type { EditorLanguage } from 'monaco-editor/esm/metadata'
  import PipelineEditorStatusBar from '$lib/components/layout/pipelines/PipelineEditorStatusBar.svelte'
  import { page } from '$app/state'
  import { useSkeletonTheme } from '$lib/compositions/useSkeletonTheme.svelte'
  import { pipelineFileNameRegex } from '$lib/compositions/health/systemErrors'
  import { effectMonacoContentPlaceholder } from '$lib/components/monacoEditor/effectMonacoContentPlaceholder.svelte'
  import { GenericOverlayWidget } from '$lib/components/monacoEditor/GenericOverlayWidget'
  import { usePipelineActionCallbacks } from '$lib/compositions/pipelines/usePipelineActionCallbacks.svelte'
  import { useCodeEditorSettings } from '$lib/compositions/pipelines/useCodeEditorSettings.svelte'
  import { rgbToHex } from '$lib/functions/common/color'

  void MonacoImports // Explicitly import all monaco-editor esm modules

  let {
    path,
    files,
    currentFileName = $bindable(),
    editDisabled,
    codeEditor,
    statusBarCenter,
    statusBarEnd,
    toolBarEnd,
    fileTab,
    downstreamChanged: _downstreamChanged = $bindable(),
    saveFile: _saveFile = $bindable(() => {})
  }: {
    path: string
    files: {
      name: string
      access: { current: string }
      language?: EditorLanguage
      markers?: Record<string, editor.IMarkerData[]>
      behaviorOnConflict?: 'auto-pull' | 'auto-push' | 'promt'
      placeholder?: string
    }[]
    currentFileName: string
    editDisabled?: boolean
    codeEditor: Snippet<[textEditor: Snippet, statusBar: Snippet, isReadOnly: boolean]>
    statusBarCenter?: Snippet
    statusBarEnd?: Snippet<[downstreamChanged: boolean]>
    toolBarEnd?: Snippet<[{ saveFile: () => void }]>
    fileTab: Snippet<[text: string, onclick: () => void, isCurrent: boolean, isSaved: boolean]>
    downstreamChanged?: boolean
    saveFile?: () => void
  } = $props()

  let editorRef: editor.IStandaloneCodeEditor = $state()!
  const { editorFontSize, autoSavePipeline, showMinimap, showStickyScroll } =
    useCodeEditorSettings()

  let wait = $derived(autoSavePipeline.value ? 2000 : ('decoupled' as const))
  let file = $derived(files.find((f) => f.name === currentFileName)!)

  function isReadonlyProperty<T>(obj: T, prop: keyof T) {
    return !Object.getOwnPropertyDescriptor(obj, prop)?.['set']
  }
  let isReadOnly = $derived(editDisabled || isReadonlyProperty(file.access, 'current'))

  const getFilePath = (file: { name: string }) => path + '/' + file.name
  let filePath = $derived(getFilePath(file))
  let previousFilePath = $state<string | undefined>(undefined)

  $effect.pre(() => {
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
  })
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
      openFiles[filePath].sync.push()
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
    if (!autoSavePipeline.value) {
      return
    }
    setTimeout(() => Object.values(openFiles).forEach((file) => file.sync.push()))
  })

  $effect(() => {
    if (!editorRef) {
      return
    }
    const [, fileName, line, , column] =
      page.url.hash.match(new RegExp(`#(${pipelineFileNameRegex}):(\\d+)(:(\\d+))?`)) ?? []
    if (!line) {
      return
    }
    if (currentFileName !== fileName) {
      currentFileName = fileName
    }
    setTimeout(() => {
      editorRef.revealPosition({ lineNumber: parseInt(line), column: parseInt(column) ?? 1 })
      window.location.hash = ''
    }, 50)
  })

  let placeholderContent = $derived(file.placeholder)
  $effect(() => {
    return effectMonacoContentPlaceholder(editorRef, placeholderContent, { opacity: '70%' })
  })

  $effect(() => {
    untrack(() => pipelineActionCallbacks.add('', 'delete', dropOpenedFile))
    return () => {
      pipelineActionCallbacks.remove('', 'delete', dropOpenedFile)
    }
  })

  $effect(() => {
    _downstreamChanged = openFiles[filePath]?.sync.downstreamChanged ?? false
  })

  $effect(() => {
    _saveFile = () => openFiles[filePath].sync.push()
  })

  let conflictWidgetRef: HTMLElement = $state(undefined!)
  const darkMode = useDarkMode()
  const theme = useSkeletonTheme()
</script>

<div class="hidden" bind:this={conflictWidgetRef}>
  <div class="relative flex flex-col gap-4 p-4 bg-surface-50-950">
    <div>
      <span class="fd fd-triangle-alert text-[20px] text-warning-500"> </span>
      The pipeline code was changed outside this window since you started editing.<br />
      Please resolve the conflict to save your changes.
    </div>
    <div class="flex flex-nowrap justify-end gap-4">
      <button
        class=" !rounded-0 px-2 py-1 bg-surface-100-900 hover:preset-outlined-primary-500"
        onclick={() => openFiles[filePath].sync.pull()}
      >
        Accept Remote
      </button>
      <button
        class=" !rounded-0 px-2 py-1 bg-surface-100-900 hover:preset-outlined-primary-500"
        onclick={() => openFiles[filePath].sync.push()}
      >
        Accept Local
      </button>
    </div>
  </div>
</div>

{@render codeEditor(textEditor, statusBar, isReadOnly)}
{#snippet textEditor()}
  <div class="flex h-full flex-col">
    <div class="flex flex-row-reverse flex-wrap items-end">
      {@render toolBarEnd?.({ saveFile: _saveFile })}
      <div class="mr-auto flex flex-nowrap items-center justify-end">
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
          onready={(editorRef) => {
            editorRef.addCommand(KeyMod.CtrlCmd | KeyCode.KeyS, () => {
              openFiles[filePath].sync.push()
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
          options={{
            readOnlyMessage: {
              value: editDisabled
                ? 'Cannot edit code while pipeline is running'
                : 'Cannot edit a compiler-generated file'
            },
            fontFamily: theme.config.monospaceFontFamily,
            fontSize: editorFontSize.value,
            theme: [
              'feldera-dark-disabled',
              'feldera-dark',
              'feldera-light-disabled',
              'feldera-light'
            ][+(darkMode.current === 'light') * 2 + +!isReadOnly],
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
  <div class="flex h-9 flex-nowrap gap-3">
    <!-- <PipelineEditorStatusBar
      {autoSavePipeline}
      downstreamChanged={openFiles[filePath].sync.downstreamChanged}
      saveCode={() => openFiles[filePath].sync.push()}
    ></PipelineEditorStatusBar> -->
    {@render statusBarCenter?.()}
  </div>
  <div class="ml-auto flex flex-nowrap gap-x-2">
    {@render statusBarEnd?.(openFiles[filePath].sync.downstreamChanged)}
  </div>
{/snippet}
