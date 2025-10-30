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
  import CodeEditor from '$lib/components/pipelines/editor/CodeEditor.svelte'
  import type { WritablePipeline } from '$lib/compositions/useWritablePipeline.svelte'
  import { untrack, type Snippet } from 'svelte'
  import { page } from '$app/state'
  import UnsavedPipelineChanges from './UnsavedPipelineChanges.svelte'

  let {
    pipeline,
    editCodeDisabled,
    children,
    statusBarCenter,
    statusBarEnd
  }: {
    pipeline: WritablePipeline<true>
    editCodeDisabled: boolean
    children?: Snippet<[editor: Snippet]>
    statusBarCenter: Snippet
    statusBarEnd: Snippet
  } = $props()

  const { updatePipelines, updatePipeline } = useUpdatePipelineList()

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

  // Reference to CodeEditor component to access closeFile method
  let codeEditorRef: CodeEditor | undefined = $state(undefined)

  // Callback for pipeline deletion - closes all files associated with the pipeline
  const dropPipelineFileState = async (deletedPipelineName: string) => {
    if (!codeEditorRef) {
      return
    }
    const fileNames = ['program.sql', 'stubs.rs', 'udf.rs', 'udf.toml']
    for (const fileName of fileNames) {
      const filePath = `${deletedPipelineName}/${fileName}`
      codeEditorRef.closeFile(filePath)
    }
  }

  // Register the deletion callback
  $effect(() => {
    untrack(() => pipelineActionCallbacks.add('', 'delete', dropPipelineFileState))
    return () => {
      untrack(() => pipelineActionCallbacks.remove('', 'delete', dropPipelineFileState))
    }
  })

  // Handle URL hash-based line reveal
  $effect(() => {
    if (!codeEditorRef) {
      return
    }
    const [, fileName, line, , column] =
      page.url.hash.match(new RegExp(`#(${pipelineFileNameRegex}):(\\d+)(:(\\d+))?`)) ?? []
    if (!line) {
      return
    }
    codeEditorRef.revealLine(fileName, parseInt(line), column ? parseInt(column) : undefined)
    window.location.hash = ''
  })
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
>
  {#snippet codeEditor(textEditor, statusBar)}
    {#snippet editor()}
      <div class="flex h-full flex-col rounded-container px-4 py-2 bg-surface-50-950">
        {@render textEditor()}
        <div
          class="bg-white-dark mb-2 flex flex-wrap items-center gap-x-8 rounded-b border-t p-2 pl-4 border-surface-50-950"
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
      class=" flex flex-nowrap py-2 pl-2 pr-5 font-medium sm:pl-3 {isCurrent
        ? 'inset-y-2 border-b-2 pb-1.5 border-surface-950-50'
        : ' rounded hover:!bg-opacity-50 hover:bg-surface-100-900'}"
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

<!-- This component prevents leaving the page without acknowledging that the unsaved changes will be lost. -->
<!-- TODO: Previously leaving a pipeline was possible without losing the changes, which would be kept in memory,
     but now if left, the temporary changes are not handled correctly when switching pipelines -->
<UnsavedPipelineChanges
  unsavedChanges={downstreamChanged}
  cleanup={() => {
    dropPipelineFileState(pipelineName)
  }}
></UnsavedPipelineChanges>
