<script lang="ts">
  import { base } from '$app/paths'
  import { type ExtendedPipeline, type Pipeline } from '$lib/services/pipelineManager'
  import { goto } from '$app/navigation'
  import { useGlobalDialog } from '$lib/compositions/useGlobalDialog.svelte'
  import Popup from '$lib/components/common/Popup.svelte'
  import { fade } from 'svelte/transition'
  import DeleteDialog, { deleteDialogProps } from '$lib/components/dialogs/DeleteDialog.svelte'
  import JSONDialog from '$lib/components/dialogs/JSONDialog.svelte'
  import JSONbig from 'true-json-bigint'
  import { Tooltip } from '$lib/components/common/Tooltip.svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'

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
  const api = usePipelineManager()
  const deletePipeline = async (pipelineName: string) => {
    await api.deletePipeline(pipelineName)
    onDeletePipeline?.(pipelineName)
    goto(`${base}/`)
  }
</script>

<Popup>
  {#snippet trigger(toggle)}
    <button
      class="fd fd-more_horiz btn btn-icon text-[20px] preset-tonal-surface"
      onclick={toggle}
      aria-label="Pipeline actions"
    ></button>
  {/snippet}
  {#snippet content(close)}
    <div
      transition:fade={{ duration: 100 }}
      class="absolute right-0 z-30 max-h-[400px] w-[calc(100vw-100px)] max-w-[300px]"
    >
      <div class="bg-white-dark flex flex-col justify-center gap-2 rounded-container p-2 shadow-md">
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
          <Tooltip class="z-10 bg-white text-surface-950-50 dark:bg-black" placement="top">
            Stop the pipeline and clear storage to <br /> change compilation profile
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
          <Tooltip class="z-10 bg-white text-surface-950-50 dark:bg-black" placement="top">
            Stop the pipeline and clear storage to <br /> allocate runtime resources
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
    filePath="file://feldera/pipelines/{pipeline.current.name}/{field}.json"
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
