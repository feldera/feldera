<script lang="ts">
  import { toast } from 'svelte-french-toast'
  import JSONbig from 'true-json-bigint'
  import IconLoader from '$assets/icons/generic/loader-alt.svg?component'
  import GenericDialog from '$lib/components/dialogs/GenericDialog.svelte'
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

  const goBackToConfig = () => {
    globalDialog.dialog = pipelineConfigurationsDialog
  }

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
      toastError('Applying pipeline config')(e as any)
      throw e
    }
    try {
      await pipeline.patch(patch)
      globalDialog.dialog = null
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
    disabledMessage="Stop the pipeline to edit settings"
    title={`Configure ${pipeline.current.name} pipeline`}
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
  ></MultiJSONDialog>
{/snippet}

{#snippet clearStorageDialog()}
  {#if clearStoragePhase === 'confirm'}
    <GenericDialog
      content={{
        title: 'Clear storage to apply changes?',
        description:
          'Storage must be cleared to apply these configuration changes. This will delete all checkpoints.',
        onSuccess: {
          name: 'Clear and apply',
          callback: doClearAndApply,
          'data-testid': 'button-confirm-clear-storage'
        },
        onCancel: {
          name: 'Back',
          callback: goBackToConfig
        }
      }}
      danger
      noclose
    ></GenericDialog>
  {:else if clearStoragePhase === 'progress'}
    <GenericDialog
      content={{
        title: 'Applying configuration changes',
        onSuccess: {
          name: 'Continue in background',
          callback: () => {
            globalDialog.dialog = null
          }
        }
      }}
    >
      <div class="flex items-center gap-2">
        <IconLoader class="h-5 flex-none animate-spin fill-surface-950-50"></IconLoader>
        <span>{clearStorageProgressMessage}</span>
      </div>
    </GenericDialog>
  {:else}
    <GenericDialog
      content={{
        title: 'Failed to apply changes',
        onSuccess: {
          name: 'Try again',
          callback: doClearAndApply
        },
        onCancel: {
          name: 'Back',
          callback: goBackToConfig
        }
      }}
      noclose
    >
      <span class="whitespace-pre-wrap text-error-500">{clearStorageError}</span>
    </GenericDialog>
  {/if}
{/snippet}
