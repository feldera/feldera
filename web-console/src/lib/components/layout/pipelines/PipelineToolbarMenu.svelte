<script lang="ts">
  import { PaneGroup, Pane, PaneResizer } from 'paneforge'
  import InteractionsPanel from '$lib/components/pipelines/editor/InteractionsPanel.svelte'
  import DeploymentStatus from '$lib/components/pipelines/list/DeploymentStatus.svelte'
  import IconLayputPanelRight from '$assets/icons/generic/layout-panel-right.svg?component'
  import PipelineActions from '$lib/components/pipelines/list/Actions.svelte'
  import { base } from '$app/paths'
  import {
    extractProgramErrors,
    programErrorReport,
    programErrorsPerFile
  } from '$lib/compositions/health/systemErrors'
  import { extractErrorMarkers, felderaCompilerMarkerSource } from '$lib/functions/pipelines/monaco'
  import {
    postPipelineAction,
    type ExtendedPipeline,
    type Pipeline,
    type PipelineAction,
    type PipelineThumb
  } from '$lib/services/pipelineManager'
  import { isPipelineIdle } from '$lib/functions/pipelines/status'
  import { nonNull } from '$lib/functions/common/function'
  import { useUpdatePipelineList } from '$lib/compositions/pipelines/usePipelineList.svelte'
  import { usePipelineActionCallbacks } from '$lib/compositions/pipelines/usePipelineActionCallbacks.svelte'
  import { useAggregatePipelineStats } from '$lib/compositions/useAggregatePipelineStats.svelte'
  import CodeEditor from '$lib/components/pipelines/editor/CodeEditor.svelte'
  import ProgramStatus from '$lib/components/pipelines/editor/ProgramStatus.svelte'
  import PipelineBreadcrumbs from '$lib/components/layout/PipelineBreadcrumbs.svelte'
  import PipelineStatus from '$lib/components/pipelines/list/PipelineStatus.svelte'
  import TabAdHocQuery from '$lib/components/pipelines/editor/TabAdHocQuery.svelte'
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'
  import AppHeader from '$lib/components/layout/AppHeader.svelte'
  import { goto } from '$app/navigation'
  import { deletePipeline as _deletePipeline } from '$lib/services/pipelineManager'
  import { useGlobalDialog } from '$lib/compositions/useGlobalDialog.svelte'
  import Popup from '$lib/components/common/Popup.svelte'
  import { fade } from 'svelte/transition'
  import { Switch } from '@skeletonlabs/skeleton-svelte'
  import DeleteDialog, { deleteDialogProps } from '$lib/components/dialogs/DeleteDialog.svelte'
  import JSONDialog from '$lib/components/dialogs/JSONDialog.svelte'
  import JSONbig from 'true-json-bigint'
  import { Tooltip } from '$lib/components/common/Tooltip.svelte'

  let {
    pipelineName,
    pipeline,
    saveFile,
    pipelineBusy,
    downstreamChanged,
    onDeletePipeline
  }: {
    pipelineName: string
    pipeline: {
      current: ExtendedPipeline
      patch: (pipeline: Partial<Pipeline>) => Promise<ExtendedPipeline>
    }
    saveFile: () => void
    pipelineBusy: boolean
    downstreamChanged: boolean
    onDeletePipeline: (pipelineName: string) => void
  } = $props()

  const globalDialog = useGlobalDialog()
  const deletePipeline = async (pipelineName: string) => {
    await _deletePipeline(pipelineName)
    onDeletePipeline?.(pipelineName)
    goto(`${base}/`)
  }
</script>

<Popup>
  {#snippet trigger(toggle)}
    <button
      class="fd fd-more_horiz btn btn-icon text-[24px] preset-tonal-surface"
      onclick={toggle}
      aria-label="Pipeline actions"
    ></button>
  {/snippet}
  {#snippet content(close)}
    <div
      transition:fade={{ duration: 100 }}
      class="absolute left-0 z-20 max-h-[400px] w-[calc(100vw-100px)] max-w-[300px]"
    >
      <div
        class="bg-white-black flex flex-col justify-center gap-2 rounded-container p-2 shadow-md"
      >
        <button
          onclick={saveFile}
          class="flex justify-between rounded p-2 hover:preset-tonal-surface {downstreamChanged
            ? ''
            : 'disabled'}"
        >
          {downstreamChanged ? 'Save' : 'Saved'}
          <span class="text-surface-500">Ctrl + S</span>
        </button>
        <div class="">
          <button
            disabled={pipelineBusy}
            class="w-full rounded p-2 text-start hover:preset-tonal-surface"
            onclick={() => (globalDialog.dialog = compilationDialog)}
          >
            Program compilation profile
          </button>
        </div>
        {#if pipelineBusy}
          <Tooltip class="z-20 bg-white text-surface-950-50 dark:bg-black" placement="top">
            Stop the pipeline to <br /> change compilation profile
          </Tooltip>
        {/if}
        <div>
          <button
            disabled={pipelineBusy}
            class="w-full rounded p-2 text-start hover:preset-tonal-surface"
            onclick={() => (globalDialog.dialog = resourcesDialog)}
          >
            Pipeline runtime resources
          </button>
        </div>
        {#if pipelineBusy}
          <Tooltip class="z-20 bg-white text-surface-950-50 dark:bg-black" placement="top">
            Stop the pipeline to <br /> allocate runtime resources
          </Tooltip>
        {/if}
        <input
          type="text"
          onkeydown={async (e) => {
            if (e.key === 'Enter') {
              const newPipelineName = e.currentTarget.value
              pipeline.patch({ name: newPipelineName }).then(() => {
                goto(`${base}/pipelines/${newPipelineName}`)
              })
            }
          }}
          enterkeyhint="done"
          placeholder="Enter a new pipeline name"
          class="input [&:not(:hover)]:ring-0"
        />

        <button
          class="rounded p-2 text-start text-error-500 hover:preset-outlined-error-500"
          onclick={() => (globalDialog.dialog = deleteDialog)}
        >
          Delete
        </button>
      </div>
    </div>
  {/snippet}
</Popup>

{#snippet deleteDialog()}
  <DeleteDialog
    {...deleteDialogProps(
      'Delete',
      (name) => `${name} pipeline`,
      (name: string) => {
        deletePipeline(name)
      }
    )(pipelineName)}
    onClose={() => (globalDialog.dialog = null)}
  ></DeleteDialog>
{/snippet}

{#snippet pipelineResourcesDialog(dialogTitle: string, field: keyof typeof pipeline.current)}
  <JSONDialog
    disabled={pipelineBusy}
    json={JSONbig.stringify(pipeline.current[field], undefined, '  ')}
    onApply={async (json) => {
      await pipeline.patch({
        [field]: JSONbig.parse(json)
      })
    }}
    onClose={() => (globalDialog.dialog = null)}
  >
    {#snippet title()}
      <div class="h5 text-center font-normal">
        {dialogTitle}
      </div>
    {/snippet}
  </JSONDialog>
{/snippet}
{#snippet resourcesDialog()}
  {@render pipelineResourcesDialog(
    `Configure ${pipeline.current.name} runtime resources`,
    'runtimeConfig'
  )}
{/snippet}
{#snippet compilationDialog()}
  {@render pipelineResourcesDialog(
    `Configure ${pipeline.current.name} compilation profile`,
    'programConfig'
  )}
{/snippet}
