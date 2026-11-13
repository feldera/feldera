<script lang="ts" module>
  let currentPipelineFile: Record<string, string> = $state({})
</script>

<script lang="ts">
  import { fileTabClass } from 'common-ui'
  import { nonNull } from 'common-lib/function'
  import PipelineActions from '#lib/components/pipelines/list/Actions.svelte'
  import {
    extractProgramErrors,
    programErrorReport,
    programErrorsPerFile,
    pipelineFileNameRegex
  } from '#lib/compositions/health/systemErrors.ts'
  import {
    extractErrorMarkers,
    felderaCompilerMarkerSource
  } from '#lib/functions/pipelines/monaco.ts'
  import { type PipelineAction } from '#lib/services/pipelineManager.ts'
  import { useUpdatePipelineList } from '#lib/compositions/pipelines/usePipelineList.svelte.ts'
  import { usePipelineActionCallbacks } from '#lib/compositions/pipelines/usePipelineActionCallbacks.svelte.ts'
  import { usePermission } from '#lib/compositions/usePermission.svelte.ts'
  import CodeEditor, {
    disposeFile as disposeCodeEditorFile
  } from '#lib/components/pipelines/editor/CodeEditor.svelte'
  import type { WritablePipeline } from '#lib/compositions/useWritablePipeline.svelte.ts'
  import { untrack } from 'svelte'
  import { page } from '$app/state'
  import UnsavedPipelineChanges from './UnsavedPipelineChanges.svelte'
  import type { Snippet } from '#lib/types/svelte.ts'
  import { getRuntimeVersion } from '#lib/functions/pipelines/runtimeVersion.ts'
  import { isUpgradeRequired, isPipelineCodeEditable } from '#lib/functions/pipelines/status.ts'
  import FocusBanner from '#lib/components/pipelines/editor/FocusBanner.svelte'
  import StorageInUseBanner from '#lib/components/pipelines/editor/StorageInUseBanner.svelte'
  import PastedLinkSaveDialog from '#lib/components/pipelines/editor/PastedLinkSaveDialog.svelte'
  import {
    pastedLinkFileNames,
    usePastedLinks
  } from '#lib/compositions/pipelines/usePastedLink.svelte.ts'
  import { useGlobalDialog } from '#lib/compositions/layout/useGlobalDialog.svelte.ts'
  import { useCodeEditorSettings } from '#lib/compositions/pipelines/useCodeEditorSettings.svelte.ts'

  let {
    pipeline,
    deleted,
    children,
    statusBarCenter,
    statusBarEnd,
    toolBarStart,
    unsavedChanges = $bindable(false)
  }: {
    pipeline: WritablePipeline<true>
    deleted: boolean
    children?: Snippet<[editor: Snippet]>
    statusBarCenter: Snippet
    statusBarEnd: Snippet
    /** Rendered before the file tabs, such as the pipelines list toggle. */
    toolBarStart?: Snippet
    /** Whether the editor holds changes that are not saved yet. Read-only for the parent. */
    unsavedChanges?: boolean
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

  // Status/upgrade reasons the code (and, downstream, the config) cannot be
  // edited right now. Feeds `editConfigDisabled` for PipelineActions, which
  // applies its own `write:pipeline_config` gate on top.
  let editCodeDisabled = $derived(
    !pipeline.current ||
      deleted ||
      (nonNull(pipeline.current.status) &&
        (!isPipelineCodeEditable(pipeline.current.status) ||
          isUpgradeRequired(pipeline.current, runtimeVersion)))
  )

  // Editing pipeline code additionally needs write:pipeline_code. Kept separate
  // from editCodeDisabled so the permission does not leak into config gating.
  const codeEdit = usePermission('write:pipeline_code')
  let codeReadOnly = $derived(editCodeDisabled || !codeEdit.allowed)

  const { updatePipelines } = useUpdatePipelineList()

  const pipelineActionCallbacks = usePipelineActionCallbacks()
  const handleActionSuccess = async (pipelineName: string, action: PipelineAction) => {
    const cbs = pipelineActionCallbacks.getAll(pipelineName, action)
    await Promise.allSettled(cbs.map((cb) => cb(pipelineName)))
  }
  const handleDeletePipeline = async (pipelineName: string) => {
    updatePipelines((pipelines) => pipelines.filter((p) => p.name !== pipelineName))
    const cbs = pipelineActionCallbacks
      .getAll('', 'delete')
      .concat(pipelineActionCallbacks.getAll(pipelineName, 'delete'))
    cbs.map((cb) => cb(pipelineName))
  }

  const programErrors = $derived(
    programErrorsPerFile(
      extractProgramErrors(programErrorReport(pipeline.current))(pipeline.current)
    )
  )

  // With Rust UDFs turned off, an empty UDF file is read-only and says why, while existing
  // UDF code stays editable so that its author can remove it.
  const rustUdfsOffMessage = 'Rust UDFs are turned off by the operator of this Feldera instance'
  const rustUdfsAllowed = $derived(page.data.feldera?.config.allow_rust_udfs ?? true)

  const pastedLinks = usePastedLinks()
  let pastedLinkFiles = $derived(pastedLinks.get(pipeline.current.id))

  let files = $derived.by(() => {
    const current = pipeline.current
    const patch = pipeline.patch
    const drafts = pastedLinkFiles
    const lockUdfRs = !rustUdfsAllowed && current.programUdfRs.trim() === ''
    const lockUdfToml = !rustUdfsAllowed && current.programUdfToml.trim() === ''
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
        ),
        initialDraft: drafts?.['program.sql']
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
        access: lockUdfRs
          ? {
              get current() {
                return current.programUdfRs
              }
            }
          : {
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
        initialDraft: lockUdfRs ? undefined : drafts?.['udf.rs'],
        placeholder: lockUdfRs
          ? `// ${rustUdfsOffMessage}.`
          : `// UDF implementation in Rust.
// See function prototypes in \`stubs.rs\`

pub fn my_udf(input: String) -> Result<String, Box<dyn std::error::Error>> {
  todo!()
}`,
        readOnlyMessage: rustUdfsOffMessage
      },
      {
        name: `udf.toml`,
        access: lockUdfToml
          ? {
              get current() {
                return current.programUdfToml
              }
            }
          : {
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
        initialDraft: lockUdfToml ? undefined : drafts?.['udf.toml'],
        placeholder: lockUdfToml
          ? `# ${rustUdfsOffMessage}.`
          : `# List Rust dependencies required by udf.rs.
example = "1.0"`,
        readOnlyMessage: rustUdfsOffMessage
      }
    ]
  })

  let pipelineName = $derived(pipeline.current.name)
  // Editor state is keyed by the id, so a rename keeps open files, drafts and undo history
  let pipelineId = $derived(pipeline.current.id)

  $effect.pre(() => {
    currentPipelineFile[pipelineId] ??= 'program.sql'
  })

  let downstreamChanged = $state(false)
  $effect(() => {
    unsavedChanges = downstreamChanged
  })
  let saveFile = $state(async () => false)

  // While the pipeline has code from a link, every save asks the user to confirm.
  // Confirming clears the pipeline's entry in `usePastedLinks`, which restores the
  // user's autosave setting.
  const globalDialog = useGlobalDialog()
  let pastedLinkSaveRequest = $state<{
    fileNames: string[]
    resolve: (isConfirmed: boolean) => void
  }>()
  const confirmPastedLinkSave = () => {
    // `pipelineId` can change before the user answers.
    const confirmedPipelineId = pipelineId
    const fileNames = pastedLinkFiles ? pastedLinkFileNames(pastedLinkFiles) : []
    return new Promise<boolean>((resolve) => {
      pastedLinkSaveRequest = {
        fileNames,
        resolve: (isConfirmed) => {
          if (isConfirmed) {
            pastedLinks.clear(confirmedPipelineId)
          }
          resolve(isConfirmed)
        }
      }
      globalDialog.dialog = pastedLinkSaveDialog
    })
  }
  let confirmSave = $derived(pastedLinkFiles ? confirmPastedLinkSave : undefined)
  const { autoSaveFiles } = useCodeEditorSettings()

  // Reference to CodeEditor component to access openFile/revealLine/setSelections
  let codeEditorRef: CodeEditor | undefined = $state(undefined)

  const PIPELINE_FILE_NAMES = ['program.sql', 'stubs.rs', 'udf.rs', 'udf.toml'] as const

  // Dispose the in-memory editor state (DecoupledStateProxy + Monaco model) for
  // every file belonging to the given pipeline. Uses the module-level
  // `disposeCodeEditorFile` so it's safe to call from cleanup after the
  // CodeEditor instance has torn down.
  const dropPipelineFileState = (pipelineIdToDrop: string) => {
    for (const fileName of PIPELINE_FILE_NAMES) {
      disposeCodeEditorFile(`${pipelineIdToDrop}/${fileName}`)
    }
    delete currentPipelineFile[pipelineIdToDrop]
    // Disposing the models discards the link's code. Clear the entry, so that the
    // code does not come back when the pipeline opens again.
    pastedLinks.clear(pipelineIdToDrop)
  }
  // Delete callbacks get the pipeline name. Only the mounted pipeline has file state,
  // so compare the name with the mounted pipeline.
  const dropDeletedPipelineFileState = async (deletedPipelineName: string) => {
    if (deletedPipelineName === pipelineName) {
      dropPipelineFileState(pipelineId)
    }
  }

  // Enforce the "only the current pipeline has in-memory state" invariant:
  // when the mounted pipeline changes (navigation to another pipeline or
  // unmount), drop the previous pipeline's file state.
  $effect(() => {
    const mountedId = pipelineId
    return () => {
      untrack(() => dropPipelineFileState(mountedId))
    }
  })

  // Also drop file state when ANY pipeline is deleted elsewhere in the tree —
  // handles the case where the currently-open pipeline is deleted from a
  // different view before unmount runs.
  $effect(() => {
    untrack(() => pipelineActionCallbacks.add('', 'delete', dropDeletedPipelineFileState))
    return () => {
      untrack(() => pipelineActionCallbacks.remove('', 'delete', dropDeletedPipelineFileState))
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
  function parseSourcePosition(url: { hash: string }):
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
  path={pipelineId}
  {files}
  editDisabled={codeReadOnly}
  readOnlyMessage={!codeEdit.allowed ? 'You have read-only access' : undefined}
  bind:currentFileName={currentPipelineFile[pipelineId]}
  autoSave={autoSaveFiles.value && !pastedLinkFiles}
  {confirmSave}
  bind:downstreamChanged
  bind:saveFile
  {statusBarCenter}
  {statusBarEnd}
  {toolBarStart}
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
      <!-- No padding: the tab row's and status bar's lines run to the panel's edges, and
           each row pads its own content instead. The file tabs' 12px margins centre the
           header row, as in the tabbed panels below (`TabsPanel`). -->
      <div class="panel flex h-full flex-col p-0">
        {@render textEditor()}
        <!-- In dark mode the status bar takes the editor's near-black, so it reads as part of
             the editor above it. -->
        <div
          class="flex flex-wrap items-center gap-x-8 border-t border-line py-[5px] pr-5 pl-5 dark:border-white/[0.06] dark:bg-dark"
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
    <!-- An unsaved file gets a dot after its name. -->
    <button class={fileTabClass(isCurrent)} onclick={onClick} data-testid="btn-file-tab">
      {text}
      {#if !isSaved}
        <span class="size-1.5 shrink-0 rounded-full bg-current" title="Unsaved changes"></span>
      {/if}
    </button>
  {/snippet}
  {#snippet toolBarEnd()}
    <!-- As tall as the 40px file-tab row, so the buttons centre in it, also when they wrap
         onto a row of their own. -->
    <div class="flex min-h-10 items-center justify-end gap-4 pr-5">
      <PipelineActions
        class=""
        {pipeline}
        {deleted}
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
  "Save and continue", "Discard changes", or "Cancel". On "Save and continue",
  the panel saves all unsaved files and then navigation proceeds. If the user
  cancels the save, the page stays open.
-->
<UnsavedPipelineChanges unsavedChanges={downstreamChanged} save={() => saveFile()} />

{#snippet pastedLinkSaveDialog()}
  {#if pastedLinkSaveRequest}
    <PastedLinkSaveDialog
      fileNames={pastedLinkSaveRequest.fileNames}
      onResolve={pastedLinkSaveRequest.resolve}
    />
  {/if}
{/snippet}
