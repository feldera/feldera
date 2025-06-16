<script lang="ts">
  import {
    type ExtendedPipeline,
    type Pipeline,
    type PipelineAction,
    type PipelineStatus
  } from '$lib/services/pipelineManager'
  import { match, P } from 'ts-pattern'
  import DeleteDialog, { deleteDialogProps } from '$lib/components/dialogs/DeleteDialog.svelte'
  import { useGlobalDialog } from '$lib/compositions/useGlobalDialog.svelte'
  import JSONDialog from '$lib/components/dialogs/JSONDialog.svelte'
  import JSONbig from 'true-json-bigint'
  import { goto } from '$app/navigation'
  import { base } from '$app/paths'
  import Tooltip from '$lib/components/common/Tooltip.svelte'
  import PipelineConfigurationsPopup from '$lib/components/layout/pipelines/PipelineConfigurationsPopup.svelte'
  import IconLoader from '$assets/icons/generic/loader-alt.svg?component'
  import { useToast } from '$lib/compositions/useToastNotification'
  import { getDeploymentStatusLabel } from '$lib/functions/pipelines/status'
  import { usePremiumFeatures } from '$lib/compositions/usePremiumFeatures.svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'

  let {
    pipeline,
    onDeletePipeline,
    editConfigDisabled,
    unsavedChanges,
    onActionSuccess,
    saveFile,
    class: _class = ''
  }: {
    pipeline: {
      current: ExtendedPipeline
      patch: (pipeline: Partial<Pipeline>) => Promise<ExtendedPipeline>
      optimisticUpdate: (newPipeline: Partial<ExtendedPipeline>) => Promise<void>
    }
    onDeletePipeline?: (pipelineName: string) => void
    editConfigDisabled: boolean
    unsavedChanges: boolean
    onActionSuccess?: (pipelineName: string, action: PipelineAction | 'start_paused_start') => void
    saveFile: () => void
    class?: string
  } = $props()

  const globalDialog = useGlobalDialog()
  const api = usePipelineManager()
  const deletePipeline = async (pipelineName: string) => {
    await api.deletePipeline(pipelineName)
    onDeletePipeline?.(pipelineName)
    goto(`${base}/`)
  }
  const { toastError } = useToast()

  const actions = {
    _start,
    _start_paused,
    _start_error,
    _start_pending,
    _pause,
    _shutdown,
    _suspend,
    _delete,
    _spacer_short,
    _spacer_long,
    _spinner,
    _status_spinner,
    _configurations,
    _configureProgram,
    _configureResources,
    _saveFile,
    _unschedule
  }

  const active = $derived.by(() => {
    return match(pipeline.current.status)
      .returnType<(keyof typeof actions)[]>()
      .with('Shutdown', () => [
        '_spacer_long',
        '_start_paused',
        '_saveFile',
        '_configurations',
        '_delete'
      ])
      .with('Preparing', 'Provisioning', 'Initializing', () => [
        '_status_spinner',
        '_saveFile',
        '_configurations',
        '_shutdown'
      ])
      .with('Pausing', 'Resuming', () => [
        '_suspend',
        '_status_spinner',
        '_saveFile',
        '_configurations',
        '_shutdown'
      ])
      .with('Unavailable', () => [
        '_suspend',
        '_spacer_long',
        '_saveFile',
        '_configurations',
        '_shutdown'
      ])
      .with('Running', () => ['_suspend', '_pause', '_saveFile', '_configurations', '_shutdown'])
      .with('Paused', () => ['_suspend', '_start', '_saveFile', '_configurations', '_shutdown'])
      .with('Suspending', () => [
        '_spacer_long',
        '_status_spinner',
        '_saveFile',
        '_configurations',
        '_shutdown'
      ])
      .with('Suspended', () => [
        '_spacer_long',
        '_start_paused',
        '_saveFile',
        '_configurations',
        '_shutdown'
      ])
      .with('ShuttingDown', () => [
        '_spacer_long',
        '_status_spinner',
        '_saveFile',
        '_configurations',
        '_spacer_short'
      ])
      .with({ PipelineError: P.any }, () => ['_saveFile', '_configurations', '_shutdown'])
      .with(
        { Queued: P.any },
        { CompilingSql: P.any },
        { SqlCompiled: P.any },
        { CompilingRust: P.any },
        (cause) => [
          Object.values(cause)[0].cause === 'upgrade'
            ? ('_unschedule' as const)
            : ('_spacer_long' as const),
          '_start_pending',
          '_saveFile',
          '_configurations',
          '_delete'
        ]
      )
      .with('SqlError', 'RustError', 'SystemError', () => [
        '_spacer_long',
        '_start_error',
        '_saveFile',
        '_configurations',
        '_delete'
      ])
      .exhaustive()
  })

  const buttonClass = 'btn gap-0'
  const iconClass = 'text-[20px]'
  const shortClass = 'w-9'
  const longClass = 'w-28 sm:w-32 justify-between pl-2 gap-2 text-sm sm:text-base'
  const shortColor = 'preset-tonal-surface'
  const basicBtnColor = 'preset-filled-surface-100-900'
  const importantBtnColor = 'preset-filled-primary-500'

  const performStartAction = async (
    action: PipelineAction | 'start_paused_start',
    pipelineName: string,
    targetStatus: PipelineStatus
  ) => {
    const waitFor = await api.postPipelineAction(
      pipeline.current.name,
      action === 'start_paused_start' ? 'start_paused' : action
    )
    pipeline.optimisticUpdate({ status: targetStatus })
    waitFor().then(() => onActionSuccess?.(pipelineName, action), toastError)
  }

  let isPremium = usePremiumFeatures()
</script>

{#snippet deleteDialog()}
  <DeleteDialog
    {...deleteDialogProps(
      'Delete',
      (name) => `${name} pipeline`,
      (name: string) => {
        deletePipeline(name)
      }
    )(pipeline.current.name)}
    onClose={() => (globalDialog.dialog = null)}
  ></DeleteDialog>
{/snippet}

{#snippet shutdownDialog()}
  <DeleteDialog
    {...deleteDialogProps(
      'Shutdown',
      (name) => `${name} pipeline`,
      (pipelineName: string) => {
        return api.postPipelineAction(pipelineName, 'shutdown').then(() => {
          onActionSuccess?.(pipelineName, 'shutdown')
          pipeline.optimisticUpdate({ status: 'ShuttingDown' })
        })
      },
      'The internal state of the pipeline will be reset.'
    )(pipeline.current.name)}
    onClose={() => (globalDialog.dialog = null)}
  ></DeleteDialog>
{/snippet}

{#snippet suspendDialog()}
  <DeleteDialog
    {...deleteDialogProps(
      'Suspend',
      (name) => `${name} pipeline`,
      (pipelineName: string) => {
        return api.postPipelineAction(pipelineName, 'suspend').then(() => {
          onActionSuccess?.(pipelineName, 'suspend')
          pipeline.optimisticUpdate({ status: 'Suspending' })
        })
      },
      "The pipeline's state will be preserved in persistent storage. Pipeline's compute resources will be released until it is resumed."
    )(pipeline.current.name)}
    onClose={() => (globalDialog.dialog = null)}
  ></DeleteDialog>
{/snippet}

<div class={'flex flex-nowrap gap-2 sm:gap-4 ' + _class}>
  {#each active as name}
    {@render actions[name]()}
  {/each}
  <!-- {@render _saveFile()}
  {@render _configurations()}
  {@render _delete()} -->
</div>

{#snippet _delete()}
  <div>
    <button
      class="{buttonClass} {shortClass} {shortColor} fd fd-trash-2 preset-tonal-surface {iconClass}"
      class:disabled={editConfigDisabled}
      onclick={() => (globalDialog.dialog = deleteDialog)}
    >
    </button>
  </div>
  {#if editConfigDisabled}
    <Tooltip
      class="bg-white-dark z-20 whitespace-nowrap rounded text-surface-950-50"
      placement="top"
    >
      Shutdown the pipeline to delete it
    </Tooltip>
  {/if}
{/snippet}
{#snippet start(
  text: string,
  getAction: (alt: boolean) => PipelineAction | 'start_paused_start',
  status: PipelineStatus
)}
  <div>
    <button
      aria-label={getAction(false)}
      class:disabled={unsavedChanges}
      class="{buttonClass} {longClass} {importantBtnColor}"
      onclick={async (e) => {
        const action = getAction(e.ctrlKey || e.shiftKey || e.metaKey)
        const pipelineName = pipeline.current.name
        performStartAction(action, pipelineName, status)
      }}
    >
      <span class="fd fd-play {iconClass}"></span>
      {text}
      <span></span>
    </button>
  </div>
{/snippet}
{#snippet _start()}
  {@render start('Resume', () => 'start', 'Resuming')}
  {#if unsavedChanges}
    <Tooltip class="bg-white-dark z-20 rounded text-surface-950-50" placement="top">
      Save the program before running
    </Tooltip>
  {/if}
{/snippet}
{#snippet _start_paused()}
  {@render start(
    pipeline.current.status === 'Suspended' ? 'Resume' : 'Start',
    (alt) => (alt ? 'start_paused' : 'start_paused_start'),
    'Preparing'
  )}
  {#if unsavedChanges}
    <Tooltip class="bg-white-dark z-20 rounded text-surface-950-50" placement="top">
      Save the program before running
    </Tooltip>
  {/if}
{/snippet}
{#snippet _start_disabled()}
  <div class="h-9">
    <button class="{buttonClass} {longClass} disabled {importantBtnColor}">
      <span class="fd fd-play {iconClass}"></span>
      Start
      <span></span>
    </button>
  </div>
{/snippet}
{#snippet _start_error()}
  {@render _start_disabled()}
  <Tooltip class="bg-white-dark z-20 rounded text-surface-950-50" placement="top">
    Resolve errors before running
  </Tooltip>
{/snippet}
{#snippet _start_pending()}
  {@render _start_disabled()}
  <Tooltip class="bg-white-dark z-20 rounded text-surface-950-50" placement="top">
    Wait for compilation to complete
  </Tooltip>
{/snippet}
{#snippet _pause()}
  <button
    class="{buttonClass} {longClass} {basicBtnColor}"
    onclick={async () => {
      const pipelineName = pipeline.current.name
      await api.postPipelineAction(pipelineName, 'pause')
      onActionSuccess?.(pipelineName, 'pause')
      pipeline.optimisticUpdate({ status: 'Pausing' })
    }}
  >
    <span class="fd fd-pause {iconClass}"></span>
    Pause
    <span></span>
  </button>
{/snippet}
{#snippet _suspend()}
  <div>
    <button
      disabled={!isPremium.value}
      class="{buttonClass} {longClass} {basicBtnColor}"
      onclick={() => {
        globalDialog.dialog = suspendDialog
      }}
    >
      <span class="fd fd-circle-stop {iconClass}"></span>
      Suspend
      <span></span>
    </button>
  </div>
  {#if !isPremium.value}
    <Tooltip
      activeContent
      class="bg-white-dark z-20 rounded text-base text-surface-950-50"
      placement="bottom"
    >
      Suspending pipelines is only available in the Enterprise edition.<br />
      <a
        class="block pt-2 underline"
        href="https://calendly.com/d/cqnj-p63-mbq/feldera-demo"
        target="_blank"
        rel="noreferrer">Upgrade</a
      >
    </Tooltip>
  {/if}
{/snippet}
{#snippet _shutdown()}
  <div>
    <button
      class="{buttonClass} {shortClass} {shortColor} fd fd-square-power {iconClass}"
      onclick={() => (globalDialog.dialog = shutdownDialog)}
    >
    </button>
  </div>
  <Tooltip class="bg-white-dark z-20 rounded text-surface-950-50" placement="top">Shutdown</Tooltip>
{/snippet}
{#snippet _saveFile()}
  <div role="button">
    <button
      class="{buttonClass} {shortClass} {shortColor} fd fd-save {iconClass}"
      class:disabled={!unsavedChanges}
      onclick={saveFile}
    >
    </button>
  </div>
  <Tooltip class="bg-white-dark z-20 rounded text-surface-950-50" placement="top">
    {#if unsavedChanges}
      Save file: Ctrl + S
    {:else}
      File saved
    {/if}
  </Tooltip>
{/snippet}
{#snippet _unschedule()}
  <button
    class="{buttonClass} {longClass} {basicBtnColor}"
    onclick={() => {
      globalDialog.dialog = shutdownDialog
    }}
  >
    <div></div>
    <div></div>
    Cancel start
    <div></div>
  </button>
  <Tooltip class="bg-white-dark z-20 whitespace-nowrap rounded text-surface-950-50" placement="top">
    The pipeline is scheduled to start automatically after compilation
  </Tooltip>
{/snippet}

{#snippet pipelineResourcesDialog(dialogTitle: string, field: keyof typeof pipeline.current)}
  <JSONDialog
    disabled={editConfigDisabled}
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
{#snippet _configureResources()}
  <button
    onclick={() => (globalDialog.dialog = resourcesDialog)}
    class="{buttonClass} {shortClass} {shortColor} fd fd-sliders-horizontal {basicBtnColor} {iconClass}"
  >
  </button>
  {#if editConfigDisabled}
    <Tooltip class="z-20 bg-white text-surface-950-50 dark:bg-black" placement="top">
      Stop the pipeline to edit configuration
    </Tooltip>
  {/if}
{/snippet}
{#snippet _configureProgram()}
  <button
    onclick={() => (globalDialog.dialog = compilationDialog)}
    class="{buttonClass} {shortClass} {shortColor} fd fd-settings {basicBtnColor} {iconClass}"
  >
  </button>
  {#if editConfigDisabled}
    <Tooltip class="z-20 bg-white text-surface-950-50 dark:bg-black" placement="top">
      Stop the pipeline to edit configuration
    </Tooltip>
  {/if}
{/snippet}
{#snippet _configurations()}
  <PipelineConfigurationsPopup {pipeline} pipelineBusy={editConfigDisabled}
  ></PipelineConfigurationsPopup>
  {#if editConfigDisabled}
    <Tooltip class="z-20 bg-white text-surface-950-50 dark:bg-black" placement="top">
      Stop the pipeline to edit configuration
    </Tooltip>
  {:else}
    <Tooltip class="z-20 bg-white text-surface-950-50 dark:bg-black" placement="top">
      Compilation and runtime configuration
    </Tooltip>
  {/if}
{/snippet}
{#snippet _spacer_short()}
  <div class={shortClass}></div>
{/snippet}
{#snippet _spacer_long()}
  <div class={longClass}></div>
{/snippet}
{#snippet _spinner()}
  <IconLoader class="pointer-events-none h-5 animate-spin fill-surface-950-50"></IconLoader>
{/snippet}
{#snippet _status_spinner()}
  <button class="{buttonClass} {longClass} pointer-events-none {basicBtnColor}">
    <IconLoader class="h-5 flex-none animate-spin fill-surface-950-50"></IconLoader>
    <span>{getDeploymentStatusLabel(pipeline.current.status)}</span>
    <span></span>
  </button>
{/snippet}
