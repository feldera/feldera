<script lang="ts">
  import JSONbig from 'true-json-bigint'
  import { toast } from 'svelte-french-toast'
  import IconLoader from '$assets/icons/generic/loader-alt.svg?component'
  import MultiJSONDialog from '$lib/components/dialogs/MultiJSONDialog.svelte'
  import { useGlobalDialog } from '$lib/compositions/layout/useGlobalDialog.svelte'
  import { getPipelineAction } from '$lib/compositions/usePipelineAction.svelte'
  import { useToast } from '$lib/compositions/useToastNotification'
  import type { WritablePipeline } from '$lib/compositions/useWritablePipeline.svelte'
  import { deletePipeline as _deletePipeline, type Pipeline } from '$lib/services/pipelineManager'

  const {
    pipeline,
    pipelineBusy
  }: {
    pipeline: WritablePipeline
    pipelineBusy: boolean
  } = $props()

  const globalDialog = useGlobalDialog()
  const { toastError } = useToast()
  const { postPipelineAction } = getPipelineAction()

  const isStorageNotClearedError = (e: unknown): boolean =>
    e instanceof Error && e.message.includes('not allowed while storage is not cleared')

  let clearStoragePhase: 'confirm' | 'progress' | 'error' = $state('confirm')
  let clearStorageProgressMessage = $state('Clearing storage...')
  let clearStorageError = $state('')
  let pendingPatch: Partial<Pipeline> | null = $state(null)
  let pendingJsonValues: Record<string, string> | null = $state(null)

  const showClearStorageConfirm = (patch: Partial<Pipeline>) => {
    pendingPatch = patch
    clearStoragePhase = 'confirm'
    clearStorageError = ''
    globalDialog.dialog = clearStorageDialog
  }

  const doClearAndApply = async () => {
    clearStoragePhase = 'progress'
    clearStorageProgressMessage = 'Clearing storage...'
    try {
      const { waitFor } = await postPipelineAction(pipeline.current.name, 'clear')
      const cleared = await waitFor()
      if (!cleared) {
        throw new Error('Failed to clear storage')
      }
      clearStorageProgressMessage = 'Applying changes...'
      await pipeline.patch(pendingPatch!)
      if (globalDialog.dialog === clearStorageDialog) {
        globalDialog.dialog = null
      }
    } catch (e) {
      if (globalDialog.dialog === clearStorageDialog) {
        clearStoragePhase = 'error'
        clearStorageError = (e as Error).message
      } else {
        toastError(e as Error)
      }
    }
  }

  const applyConfig = async (json: Record<string, string>) => {
    let patch: Partial<Pipeline> = {}
    try {
      patch.runtimeConfig = JSONbig.parse(json.runtimeConfig)
      patch.programConfig = JSONbig.parse(json.programConfig)
    } catch (e) {
      toastError(e as any)
      throw e
    }
    try {
      await pipeline.patch(patch)
    } catch (e) {
      if (isStorageNotClearedError(e)) {
        toast.dismiss()
        pendingJsonValues = json
        showClearStorageConfirm(patch)
      }
      throw e
    }
  }
</script>

<button
  class="fd fd-settings btn-icon preset-tonal-surface text-[20px]"
  onclick={() => {
    pendingJsonValues = null
    globalDialog.dialog = pipelineConfigurationsDialog
  }}
  aria-label="Pipeline actions"
></button>
<!-- <button
  class="group flex flex-nowrap items-center gap-2 rounded px-2 text-[20px] preset-tonal-surface hover:preset-filled-surface-100-900 sm:gap-3 sm:px-3"
  onclick={() => (globalDialog.dialog = pipelineConfigurationsDialog)}
  aria-label="Pipeline actions"
>
  <div class="fd fd-settings group-hover:preset-filled-surface-100-900"></div>
  {#if pipelineBusy}
    <Tooltip class="z-20 rounded bg-white text-surface-950-50 dark:bg-black" placement="top">
      Stop the pipeline to edit settings
    </Tooltip>
  {:else}
    <Tooltip class="z-20 rounded bg-white text-surface-950-50 dark:bg-black" placement="top">
      Compilation and runtime configuration
    </Tooltip>
  {/if}
  <span class="w-0 -translate-x-[1px] text-base text-surface-500">|</span>
  <div class="fd rotate-90 text-surface-500 {storageBound ? 'fd-mic' : 'fd-mic-off'}"></div>

  <Tooltip class="bg-white-dark z-20 rounded text-surface-950-50">
    {#if storageBound}
      Storage is in use, click to change
    {:else}
      Storage is not in use, click to change
    {/if}
  </Tooltip>
</button> -->

{#snippet pipelineConfigurationsDialog()}
  <MultiJSONDialog
    disabled={pipelineBusy}
    refreshOnChange={!pendingJsonValues}
    values={pendingJsonValues ?? {
      runtimeConfig: JSONbig.stringify(pipeline.current.runtimeConfig, undefined, '  '),
      programConfig: JSONbig.stringify(pipeline.current.programConfig, undefined, '  ')
    }}
    metadata={{
      runtimeConfig: {
        title: `Runtime configuration`,
        editorClass: 'h-[40vh]',
        filePath: `file://pipelines/${pipeline.current.name}/RuntimeConfig.json`,
        readOnlyMessage: 'Cannot edit config while pipeline is running'
      },
      programConfig: {
        title: `Compilation configuration`,
        editorClass: 'h-24',
        filePath: `file://pipelines/${pipeline.current.name}/ProgramConfig.json`,
        readOnlyMessage: 'Cannot edit config while pipeline is running'
      }
    }}
    onApply={applyConfig}
    onClose={() => {
      pendingJsonValues = null
      globalDialog.dialog = null
    }}
  >
    {#snippet title()}
      Configure {pipeline.current.name} pipeline
    {/snippet}
  </MultiJSONDialog>
{/snippet}

{#snippet clearStorageDialog()}
  <div class="p-4 sm:p-8">
    {#if clearStoragePhase === 'confirm'}
      <div class="flex flex-col gap-4">
        <div class="flex flex-nowrap justify-between">
          <div class="h5">Clear storage to apply changes?</div>
          <button
            class="fd fd-x -m-4 btn-icon text-[24px]"
            onclick={() => (globalDialog.dialog = null)}
            aria-label="Close"
          ></button>
        </div>
        <span
          >Storage must be cleared to apply these configuration changes. This will
          delete all checkpoints.</span
        >
      </div>
      <div class="flex flex-col-reverse gap-4 pt-4 sm:flex-row sm:justify-end">
        <button
          class="btn preset-filled-surface-50-950 px-4"
          onclick={() => (globalDialog.dialog = pipelineConfigurationsDialog)}>Back</button
        >
        <button
          class="btn preset-filled-error-500 px-4 font-semibold"
          onclick={doClearAndApply}
          data-testid="button-confirm-clear-storage">Clear and apply</button
        >
      </div>
    {:else if clearStoragePhase === 'progress'}
      <div class="flex flex-col gap-4">
        <div class="h5">Applying configuration changes</div>
        <div class="flex items-center gap-2">
          <IconLoader class="h-5 flex-none animate-spin fill-surface-950-50"></IconLoader>
          <span>{clearStorageProgressMessage}</span>
        </div>
      </div>
      <div class="flex justify-end pt-4">
        <button
          class="btn preset-filled-surface-50-950 px-4"
          onclick={() => (globalDialog.dialog = null)}>Continue in background</button
        >
      </div>
    {:else}
      <div class="flex flex-col gap-4">
        <div class="flex flex-nowrap justify-between">
          <div class="h5">Failed to apply changes</div>
          <button
            class="fd fd-x -m-4 btn-icon text-[24px]"
            onclick={() => (globalDialog.dialog = null)}
            aria-label="Close"
          ></button>
        </div>
        <span class="whitespace-pre-wrap text-error-500">{clearStorageError}</span>
      </div>
      <div class="flex justify-end pt-4">
        <button
          class="btn preset-filled-surface-50-950 px-4"
          onclick={() => (globalDialog.dialog = null)}>Close</button
        >
      </div>
    {/if}
  </div>
{/snippet}
