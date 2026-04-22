<script lang="ts" module>
  let currentPipelineFile: Record<string, string> = $state({})
</script>

<script lang="ts">
  import PipelineActions from '$lib/components/pipelines/list/Actions.svelte'
  import {
    extractProgramErrors,
    programErrorReport,
    programErrorsPerFile,
    pipelineFileNameRegex
  } from '$lib/compositions/health/systemErrors'
  import { extractErrorMarkers, felderaCompilerMarkerSource } from '$lib/functions/pipelines/monaco'
  import { type PipelineAction } from '$lib/services/pipelineManager'
  import { useUpdatePipelineList } from '$lib/compositions/pipelines/usePipelineList.svelte'
  import { usePipelineActionCallbacks } from '$lib/compositions/pipelines/usePipelineActionCallbacks.svelte'
  import CodeEditor, {
    disposeFile as disposeCodeEditorFile
  } from '$lib/components/pipelines/editor/CodeEditor.svelte'
  import type { WritablePipeline } from '$lib/compositions/useWritablePipeline.svelte'
  import { untrack } from 'svelte'
  import { page } from '$app/state'
  import UnsavedPipelineChanges from './UnsavedPipelineChanges.svelte'
  import type { Snippet } from '$lib/types/svelte'
  import { getRuntimeVersion } from '$lib/functions/pipelines/runtimeVersion'
  import { nonNull } from '$lib/functions/common/function'
  import { isUpgradeRequired, isPipelineCodeEditable } from '$lib/functions/pipelines/status'
  import FocusBanner from '$lib/components/pipelines/editor/FocusBanner.svelte'
  import StorageInUseBanner from '$lib/components/pipelines/editor/StorageInUseBanner.svelte'

  let {
    pipeline,
    deleted,
    children,
    statusBarCenter,
    statusBarEnd
  }: {
    pipeline: WritablePipeline<true>
    deleted: boolean
    children?: Snippet<[editor: Snippet]>
    statusBarCenter: Snippet
    statusBarEnd: Snippet
  } = $props()

  let runtimeVersion = $derived(
    getRuntimeVersion(
      {
        runtime: pipeline.current.platformVersion,
        base: page.data.feldera!.version,
        configured: pipeline.current.programConfig?.runtime_version
      },
      page.data.feldera!.unstableFeatures
    )
  )

  let editCodeDisabled = $derived(
    !pipeline.current ||
      deleted ||
      (nonNull(pipeline.current.status) &&
        (!isPipelineCodeEditable(pipeline.current.status) ||
          isUpgradeRequired(pipeline.current, runtimeVersion)))
  )

  const { updatePipelines } = useUpdatePipelineList()

  const pipelineActionCallbacks = usePipelineActionCallbacks()
  const handleActionSuccess = async (pipelineName: string, action: PipelineAction) => {
    const cbs = pipelineActionCallbacks.getAll(pipelineName, action)
    await Promise.allSettled(cbs.map((x) => x(pipelineName)))
  }
  const handleDeletePipeline = async (pipelineName: string) => {
    updatePipelines((pipelines) => pipelines.filter((p) => p.name !== pipelineName))
    const cbs = pipelineActionCallbacks
      .getAll('', 'delete')
      .concat(pipelineActionCallbacks.getAll(pipelineName, 'delete'))
    cbs.map((x) => x(pipelineName))
  }

  const programErrors = $derived(
    programErrorsPerFile(
      extractProgramErrors(programErrorReport(pipeline.current))(pipeline.current)
    )
  )

  let files = $derived.by(() => {
    const current = pipeline.current
    const patch = pipeline.patch
    return [
      {
        name: `program.sql`,
        access: {
          get current() {
            return current.programCode
          },
          set current(programCode: string) {
            patch({ programCode }, true)
          }
        },
        language: 'sql' as const,
        markers: ((errors) =>
          errors ? { [felderaCompilerMarkerSource]: extractErrorMarkers(errors) } : undefined)(
          programErrors['program.sql']
        )
      },
      {
        name: `stubs.rs`,
        access: {
          get current() {
            return current.programInfo?.udf_stubs ?? ''
          }
        },
        language: 'rust' as const,
        markers: ((errors) =>
          errors ? { [felderaCompilerMarkerSource]: extractErrorMarkers(errors) } : undefined)(
          programErrors['stubs.rs']
        ),
        behaviorOnConflict: 'auto-pull' as const
      },
      {
        name: `udf.rs`,
        access: {
          get current() {
            return current.programUdfRs
          },
          set current(programUdfRs: string) {
            patch({ programUdfRs }, true)
          }
        },
        language: 'rust' as const,
        markers: ((errors) =>
          errors ? { [felderaCompilerMarkerSource]: extractErrorMarkers(errors) } : undefined)(
          programErrors['udf.rs']
        ),
        placeholder: `// UDF implementation in Rust.
// See function prototypes in \`stubs.rs\`

pub fn my_udf(input: String) -> Result<String, Box<dyn std::error::Error>> {
  todo!()
}`
      },
      {
        name: `udf.toml`,
        access: {
          get current() {
            return current.programUdfToml
          },
          set current(programUdfToml: string) {
            patch({ programUdfToml }, true)
          }
        },
        language: 'graphql' as const,
        markers: ((errors) =>
          errors ? { [felderaCompilerMarkerSource]: extractErrorMarkers(errors) } : undefined)(
          programErrors['udf.toml']
        ),
        placeholder: `# List Rust dependencies required by udf.rs.
example = "1.0"`
      }
    ]
  })

  let pipelineName = $derived(pipeline.current.name)

  $effect.pre(() => {
    currentPipelineFile[pipelineName] ??= 'program.sql'
  })

  let downstreamChanged = $state(false)
  let saveFile = $state(() => {})

  // Reference to CodeEditor component to access openFile/revealLine/setSelections
  let codeEditorRef: CodeEditor | undefined = $state(undefined)

  const PIPELINE_FILE_NAMES = ['program.sql', 'stubs.rs', 'udf.rs', 'udf.toml'] as const

  // Dispose the in-memory editor state (DecoupledStateProxy + Monaco model) for
  // every file belonging to the given pipeline. Uses the module-level
  // `disposeCodeEditorFile` so it's safe to call from cleanup after the
  // CodeEditor instance has torn down.
  const dropPipelineFileState = async (pipelineNameToDrop: string) => {
    for (const fileName of PIPELINE_FILE_NAMES) {
      disposeCodeEditorFile(`${pipelineNameToDrop}/${fileName}`)
    }
    delete currentPipelineFile[pipelineNameToDrop]
  }

  // Enforce the "only the current pipeline has in-memory state" invariant:
  // when the mounted pipeline changes (navigation to another pipeline or
  // unmount), drop the previous pipeline's file state.
  $effect(() => {
    const mountedName = pipelineName
    return () => {
      untrack(() => dropPipelineFileState(mountedName))
    }
  })

  // Also drop file state when ANY pipeline is deleted elsewhere in the tree —
  // handles the case where the currently-open pipeline is deleted from a
  // different view before unmount runs.
  $effect(() => {
    untrack(() => pipelineActionCallbacks.add('', 'delete', dropPipelineFileState))
    return () => {
      untrack(() => pipelineActionCallbacks.remove('', 'delete', dropPipelineFileState))
    }
  })

  let showEditorBanner = $derived(
    pipeline.current.status === 'Stopped' && pipeline.current.storageStatus === 'InUse'
  )
  let isEditorFocused = $state(false)

  $effect(() => {
    if (!codeEditorRef) {
      return
    }

    // This effect detects when the code editor should scroll to reveal a source position or multiple source ranges
    // Supported formats:
    // 1. Simple: #filename:line:column (scroll only)
    // 2. Range(s): #filename:startLine:startColumn-endLine:endColumn,startLine2:startColumn2-endLine2:endColumn2 (multiple selections)

    const { fileName, selection } = parseSourcePosition(page.url)
    if (!selection) {
      return
    }

    codeEditorRef.openFile(fileName)

    setTimeout(() => {
      if ('ranges' in selection) {
        // Range format found - create selections for multiple source positions

        const { ranges } = selection
        codeEditorRef?.setSelections(ranges)
      } else {
        // Simple format, single position: `line:column` (scroll only)
        codeEditorRef?.revealLine(selection)
      }

      window.location.hash = ''
    }, 50)
  })

  type CodePosition = { line: number; column: number }

  /**
   * Parse source position(s) from URL anchor.
   *
   * Supported formats:
   * 1. Simple: "line:column"
   * 2. Range(s): "startLine:startColumn-endLine:endColumn,startLine2:startColumn2-endLine2:endColumn2"
   */
  function parseSourcePosition(url: URL):
    | {
        fileName: string
        selection:
          | {
              ranges: {
                start: CodePosition
                end: CodePosition
              }[]
            }
          | CodePosition
          | null
      }
    | {
        fileName: null
        selection: null
      } {
    const hashMatch = url.hash.match(new RegExp(`#(${pipelineFileNameRegex}):(.+)`))
    if (!hashMatch) {
      return {
        fileName: null,
        selection: null
      }
    }

    const [, fileName, positionString] = hashMatch

    if (!positionString) {
      return {
        fileName: null,
        selection: null
      }
    }

    // Try to match all ranges pattern: startLine:startColumn-endLine:endColumn
    const rangeMatches = [...positionString.matchAll(/(\d+):(\d+)-(\d+):(\d+)/g)]

    if (rangeMatches.length > 0) {
      return {
        fileName,
        selection: {
          ranges: rangeMatches.map((match) => {
            const [, startLine, startColumn, endLine, endColumn] = match
            return {
              start: {
                line: parseInt(startLine),
                column: parseInt(startColumn)
              },
              end: {
                line: parseInt(endLine),
                column: parseInt(endColumn)
              }
            }
          })
        }
      }
    }

    // Simple format, single position: `line:column` (scroll only)
    const parts = positionString.split(':')
    const position = {
      line: parseInt(parts[0]),
      column: parseInt(parts[1]) || 1
    }

    if (isNaN(position.line)) {
      return {
        fileName: null,
        selection: null
      }
    }

    return {
      fileName,
      selection: position
    }
  }
</script>

<CodeEditor
  bind:this={codeEditorRef}
  path={pipelineName}
  {files}
  editDisabled={editCodeDisabled}
  bind:currentFileName={currentPipelineFile[pipelineName]}
  bind:downstreamChanged
  bind:saveFile
  {statusBarCenter}
  {statusBarEnd}
  bind:isFocused={isEditorFocused}
>
  {#snippet beforeTextArea()}
    <FocusBanner show={showEditorBanner} isFocused={isEditorFocused}>
      {#snippet content()}
        <StorageInUseBanner {pipelineName} {runtimeVersion} />
      {/snippet}
    </FocusBanner>
  {/snippet}
  {#snippet codeEditor(textEditor, statusBar)}
    {#snippet editor()}
      <div class="flex h-full flex-col rounded-container bg-surface-50-950 px-4 py-2">
        {@render textEditor()}
        <div
          class="bg-white-dark mb-2 flex flex-wrap items-center gap-x-8 rounded-b border-t border-surface-50-950 p-2 pl-4"
        >
          {@render statusBar()}
        </div>
      </div>
    {/snippet}
    {#if children}
      {@render children(editor)}
    {:else}
      {@render editor()}
    {/if}
  {/snippet}
  {#snippet fileTab(text, onClick, isCurrent, isSaved)}
    <button
      class=" flex flex-nowrap py-2 pr-5 pl-2 font-medium sm:pl-3 {isCurrent
        ? 'inset-y-2 border-b-2 border-surface-950-50 pb-1.5'
        : ' hover:!bg-opacity-50 rounded hover:bg-surface-100-900'}"
      onclick={onClick}
    >
      {text}
      <div
        class="h-0 w-0 -translate-x-2 -translate-y-1.5 text-4xl {isSaved ? '' : 'fd fd-dot'}"
      ></div>
    </button>
  {/snippet}
  {#snippet toolBarEnd()}
    <div class="flex justify-end gap-4 pb-2">
      <PipelineActions
        class=""
        {pipeline}
        onDeletePipeline={handleDeletePipeline}
        editConfigDisabled={editCodeDisabled}
        unsavedChanges={downstreamChanged}
        onActionSuccess={handleActionSuccess}
        {saveFile}
      ></PipelineActions>
    </div>
  {/snippet}
</CodeEditor>

<!--
  Prevents leaving the pipeline with unsaved changes. The dialog offers
  "Save and continue", "Discard changes", or "Cancel". On save-and-continue,
  all dirty files are pushed upstream and then navigation proceeds.
-->
<UnsavedPipelineChanges unsavedChanges={downstreamChanged} save={() => saveFile()} />
