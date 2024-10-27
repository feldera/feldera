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
      'editor.background': '#ffffff'
    }
  })

  MonacoImports.editor.defineTheme('feldera-dark', {
    base: 'vs-dark',
    inherit: true,
    rules: [{ token: 'string.sql', foreground: '#d9731a' }],
    colors: {
      'editor.background': '#000000'
    }
  })

  MonacoImports.editor.defineTheme('feldera-light-disabled', {
    base: 'vs',
    inherit: true,
    rules: [{ token: 'string.sql', foreground: '#7a3d00' }],
    colors: {
      'editor.background': '#f2f2f2'
    }
  })

  MonacoImports.editor.defineTheme('feldera-dark-disabled', {
    base: 'vs-dark',
    inherit: true,
    rules: [{ token: 'string.sql', foreground: '#d9731a' }],
    colors: {
      'editor.background': '#212121'
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
  import { page } from '$app/stores'
  import { useSkeletonTheme } from '$lib/compositions/useSkeletonTheme.svelte'
  import { pipelineFileNameRegex } from '$lib/compositions/health/systemErrors'
  import { effectMonacoContentPlaceholder } from '$lib/components/monacoEditor/effectMonacoContentPlaceholder.svelte'
  import { GenericOverlayWidget } from '$lib/components/monacoEditor/GenericOverlayWidget'
  import { usePipelineActionCallbacks } from '$lib/compositions/pipelines/usePipelineActionCallbacks.svelte'

  void MonacoImports // Explicitly import all monaco-editor esm modules

  let {
    path,
    files,
    currentFileName = $bindable(),
    editDisabled,
    textEditor,
    statusBarCenter,
    statusBarEnd
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
    textEditor: Snippet<[children: Snippet]>
    statusBarCenter?: Snippet
    statusBarEnd?: Snippet<[downstreamChanged: boolean]>
  } = $props()

  let editorRef: editor.IStandaloneCodeEditor = $state()!
  const autoSavePipeline = useLocalStorage('layout/pipelines/autosave', true)
  const showMinimap = useLocalStorage('layout/pipelines/editor/minimap', true)
  const showStickyScroll = useLocalStorage('layout/pipelines/editor/stickyScroll', true)

  let wait = $derived(autoSavePipeline.value ? 2000 : ('decoupled' as const))
  let file = $derived(files.find((f) => f.name === currentFileName)!)

  function isReadonlyProperty<T>(obj: T, prop: keyof T) {
    return !Object.getOwnPropertyDescriptor(obj, prop)?.['set']
  }
  let isReadonly = $derived(editDisabled || isReadonlyProperty(file.access, 'current'))

  let filePath = $derived(path + '/' + file.name)
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
      $page.url.hash.match(new RegExp(`#(${pipelineFileNameRegex}):(\\d+)(:(\\d+))?`)) ?? []
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

  let conflictWidgetRef: HTMLElement = $state(undefined!)
  const mode = useDarkMode()
  const theme = useSkeletonTheme()
</script>

<div class="hidden" bind:this={conflictWidgetRef}>
  <div class="relative flex flex-col gap-4 p-4 bg-surface-50-950">
    <div>
      <span class="fd fd-warning_amber text-[24px] text-warning-500"> </span>
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

{@render textEditor(x)}
{#snippet x()}
  <div class="flex h-full flex-col">
    <div class="flex flex-wrap">
      {#each files as file}
        <button
          class="py-1 pl-3 pr-8 {file.name === currentFileName
            ? 'bg-white-black'
            : 'hover:!bg-opacity-50 hover:bg-surface-100-900'}"
          onclick={() => (currentFileName = file.name)}
        >
          {file.name}
        </button>
      {/each}
    </div>
    <div class="relative flex-1">
      <div class="absolute h-full w-full">
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
            fontSize: 16,
            theme: [
              'feldera-dark-disabled',
              'feldera-dark',
              'feldera-light-disabled',
              'feldera-light'
            ][+(mode.darkMode.value === 'light') * 2 + +!isReadonly],
            automaticLayout: true,
            lineNumbersMinChars: 3,
            ...isMonacoEditorDisabled(isReadonly),
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

<div class="flex flex-wrap items-center gap-x-8 border-y-[1px] pr-2 border-surface-100-900">
  <div class="flex h-9 flex-nowrap gap-2">
    <PipelineEditorStatusBar
      {autoSavePipeline}
      downstreamChanged={openFiles[filePath].sync.downstreamChanged}
      saveCode={() => openFiles[filePath].sync.push()}
    ></PipelineEditorStatusBar>
    {@render statusBarCenter?.()}
  </div>
  <div class=" ml-auto flex flex-nowrap gap-x-8">
    {@render statusBarEnd?.(openFiles[filePath].sync.downstreamChanged)}
  </div>
</div>
