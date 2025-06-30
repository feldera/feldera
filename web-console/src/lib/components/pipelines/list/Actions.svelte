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
  import { getDeploymentStatusLabel, isPipelineShutdown } from '$lib/functions/pipelines/status'
  import { usePremiumFeatures } from '$lib/compositions/usePremiumFeatures.svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import Popup from '$lib/components/common/Popup.svelte'
  import { slide } from 'svelte/transition'

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
    _kill_short,
    _kill,
    _stop,
    _multiStop,
    _delete,
    _spacer_short,
    _spacer_long,
    _spinner,
    _status_spinner,
    _configurations,
    _configureProgram,
    _configureResources,
    _saveFile,
    _unschedule,
    _unbind_storage
  }

  const active = $derived.by(() => {
    return match(pipeline.current.status)
      .returnType<(keyof typeof actions)[]>()
      .with('Stopped', () => [
        '_start_paused',
        '_saveFile',
        '_configurations',
        '_unbind_storage',
        '_delete'
      ])
      .with('Preparing', 'Provisioning', 'Initializing', () => [
        '_multiStop',
        '_spinner',
        '_saveFile',
        '_configurations',
        '_unbind_storage',
        '_delete'
      ])
      .with('Pausing', 'Resuming', () => [
        '_multiStop',
        '_spinner',
        '_saveFile',
        '_configurations',
        '_unbind_storage',
        '_delete'
      ])
      .with('Unavailable', () => [
        '_multiStop',
        '_spacer_long',
        '_saveFile',
        '_configurations',
        '_unbind_storage',
        '_delete'
      ])
      .with('Running', () => [
        '_multiStop',
        '_pause',
        '_saveFile',
        '_configurations',
        '_unbind_storage',
        '_delete'
      ])
      .with('Paused', () => [
        '_multiStop',
        '_start',
        '_saveFile',
        '_configurations',
        '_unbind_storage',
        '_delete'
      ])
      .with('Suspending', () => [
        '_spinner',
        '_saveFile',
        '_configurations',
        '_unbind_storage',
        '_delete'
      ])
      .with('Stopping', () => [
        '_spinner',
        '_saveFile',
        '_configurations',
        '_unbind_storage',
        '_delete'
      ])
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
          '_unbind_storage',
          '_delete'
        ]
      )
      .with('SqlError', 'RustError', 'SystemError', () => [
        '_spacer_long',
        '_start_error',
        '_saveFile',
        '_configurations',
        '_unbind_storage',
        '_delete'
      ])
      .exhaustive()
  })

  const buttonClass = 'btn'
  const iconClass = 'text-[20px]'
  const shortClass = 'w-9'
  const longClass = 'w-[104px] sm:w-[120px] justify-between pl-2 gap-2 text-sm sm:text-base'
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

{#snippet unbindDialog()}
  <DeleteDialog
    {...deleteDialogProps(
      'Delete',
      (name) => `${name} pipeline storage`,
      (name: string) => {
        api.postPipelineAction(name, 'unbind')
      },
      'Deleting pipeline storage will delete all checkpoints and release provisioned storage resources.'
    )(pipeline.current.name)}
    onClose={() => (globalDialog.dialog = null)}
  ></DeleteDialog>
{/snippet}

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

{#snippet killDialog()}
  <DeleteDialog
    {...deleteDialogProps(
      'Force stop',
      (name) => `${name} pipeline`,
      (pipelineName: string) => {
        return api.postPipelineAction(pipelineName, 'kill').then(() => {
          onActionSuccess?.(pipelineName, 'kill')
          pipeline.optimisticUpdate({ status: 'Stopping' })
        })
      },
      'The pipeline will stop prosessing inputs without making a checkpoint, leaving only a previous one, if any.'
    )(pipeline.current.name)}
    onClose={() => (globalDialog.dialog = null)}
  ></DeleteDialog>
{/snippet}

{#snippet stopDialog()}
  <DeleteDialog
    {...deleteDialogProps(
      'Stop',
      (name) => `${name} pipeline`,
      (pipelineName: string) => {
        return api.postPipelineAction(pipelineName, 'stop').then(() => {
          onActionSuccess?.(pipelineName, 'stop')
          pipeline.optimisticUpdate({ status: 'Suspending' })
        })
      },
      'The pipeline will stop prosessing inputs and make a checkpoint of the state (Enterprise only).'
    )(pipeline.current.name)}
    onClose={() => (globalDialog.dialog = null)}
  ></DeleteDialog>
{/snippet}

<div class={'flex flex-nowrap gap-2 sm:gap-4 ' + _class}>
  <!-- <div class="btn p-0 preset-filled-surface-100-900">
    <button class="btn flex justify-center gap-2">
      <span class="fd fd-circle-stop {iconClass}"></span>
      <span>Suspend</span>
    </button>
    <div class="-mx-2 h-full border-l-[2px] border-surface-50-950"></div>
    <button class="fd fd-chevron-down btn btn-icon text-[20px]"> </button>
  </div> -->

  {#each active as name}
    {@render actions[name]()}
  {/each}
  <!-- {@render _saveFile()}
  {@render _configurations()}
  {@render _delete()} -->
</div>

{#snippet _multiStop()}
  <Popup>
    {#snippet trigger(toggle)}
      <div class="flex flex-nowrap p-0">
        <!-- <button class="btn flex justify-center gap-2">
      <span class="fd fd-circle-stop {iconClass}"></span>
      <span>Suspend</span>
    </button> -->
        <div class="w-[120px] sm:w-[140px]">{@render (isPremium.value ? _stop : _kill)()}</div>
        <div class="z-10 -ml-6 h-9 border-l-[4px] border-surface-50-950"></div>
        <button
          onclick={toggle}
          class="fd fd-chevron-down btn btn-icon z-10 w-7 !rounded-l-none text-[20px] preset-filled-surface-100-900"
          aria-label="See stop options"
        >
        </button>
      </div>
    {/snippet}
    {#snippet content(close)}
      <div
        transition:slide={{ duration: 100 }}
        class="absolute z-30 mt-2 flex max-h-[400px] flex-col justify-stretch rounded shadow-md bg-surface-50-950 scrollbar"
      >
        {@render (isPremium.value ? _kill : _stop)()}
      </div>
    {/snippet}
  </Popup>
{/snippet}

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
      class="hidden sm:flex {buttonClass} {longClass} {importantBtnColor}"
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

    <button
      aria-label={getAction(false)}
      class:disabled={unsavedChanges}
      class="flex sm:hidden {buttonClass} {shortClass} {importantBtnColor} {iconClass}"
      onclick={async (e) => {
        const action = getAction(e.ctrlKey || e.shiftKey || e.metaKey)
        const pipelineName = pipeline.current.name
        performStartAction(action, pipelineName, status)
      }}
    >
      <span class="fd fd-play {iconClass}"></span>
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
  {@render start('Start', (alt) => (alt ? 'start_paused' : 'start_paused_start'), 'Preparing')}
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
    class="hidden sm:flex {buttonClass} {longClass} {basicBtnColor}"
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
  <button
    class="flex sm:hidden {buttonClass} {shortClass} {basicBtnColor} {iconClass}"
    onclick={async () => {
      const pipelineName = pipeline.current.name
      await api.postPipelineAction(pipelineName, 'pause')
      onActionSuccess?.(pipelineName, 'pause')
      pipeline.optimisticUpdate({ status: 'Pausing' })
    }}
  >
    <span class="fd fd-pause {iconClass}"></span>
  </button>
{/snippet}
{#snippet _stop()}
  <div>
    <button
      disabled={!isPremium.value}
      class="{buttonClass} {longClass} {basicBtnColor}"
      onclick={() => {
        globalDialog.dialog = stopDialog
      }}
    >
      <span class="fd fd-square {iconClass}"></span>
      Stop
      <span></span>
    </button>
  </div>
  {#if !isPremium.value}
    <Tooltip
      activeContent
      class="bg-white-dark z-20 w-max max-w-[90vw] rounded text-base text-surface-950-50"
      placement="bottom"
    >
      Stopping pipelines gracefully is only available in the Enterprise edition.<br />
      <a
        class="block pt-2 underline"
        href="https://calendly.com/d/cqnj-p63-mbq/feldera-demo"
        target="_blank"
        rel="noreferrer">Upgrade</a
      >
    </Tooltip>
  {/if}
{/snippet}
{#snippet _kill_short()}
  <div>
    <button
      class="{buttonClass} {shortClass} {shortColor} fd fd-square-power {iconClass}"
      onclick={() => (globalDialog.dialog = killDialog)}
    >
    </button>
  </div>
  <Tooltip class="bg-white-dark z-20 rounded text-surface-950-50" placement="top"
    >Force stop</Tooltip
  >
{/snippet}
{#snippet _kill()}
  <button
    class="{buttonClass} {longClass} {basicBtnColor}"
    onclick={() => (globalDialog.dialog = killDialog)}
  >
    <span class="fd fd-square-power {iconClass}"></span>
    Force stop
    <span></span>
  </button>
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
      globalDialog.dialog = stopDialog
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
      Stop the pipeline and unbind storage to edit configuration
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
      Stop the pipeline and unbind storage to edit configuration
    </Tooltip>
  {/if}
{/snippet}
{#snippet _configurations()}
  <PipelineConfigurationsPopup {pipeline} pipelineBusy={editConfigDisabled}
  ></PipelineConfigurationsPopup>
{/snippet}
{#snippet _spacer_short()}
  <div class={shortClass}></div>
{/snippet}
{#snippet _spacer_long()}
  <div class={longClass}></div>
{/snippet}
{#snippet _spinner()}
  <div class="flex sm:hidden">
    {@render _spinner_short()}
  </div>
  <div class="hidden sm:flex">
    {@render _status_spinner()}
  </div>
{/snippet}
{#snippet _spinner_short()}
  <div class="pointer-events-none {buttonClass} {shortClass} {basicBtnColor}">
    <IconLoader class="h-5 flex-none  animate-spin fill-surface-950-50"></IconLoader>
  </div>
{/snippet}
{#snippet _status_spinner()}
  <button class="{buttonClass} {longClass} pointer-events-none {basicBtnColor}">
    <IconLoader class="h-5 flex-none animate-spin fill-surface-950-50"></IconLoader>
    <span>{getDeploymentStatusLabel(pipeline.current.status)}</span>
    <span></span>
  </button>
{/snippet}
{#snippet _unbind_storage()}
  {@const storageStatus = pipeline.current.storageStatus}
  {@const isShutdown = isPipelineShutdown(pipeline.current.status)}
  {#if storageStatus === 'Unbinding'}
    <div class="pointer-events-none {buttonClass} {shortClass} {basicBtnColor}">
      <IconLoader class="h-5 flex-none  animate-spin fill-surface-950-50"></IconLoader>
    </div>
    <Tooltip class="z-20 bg-white text-surface-950-50 dark:bg-black" placement="top">
      The pipeline storage is being deleted, and provisioned resources deallocated.
    </Tooltip>
  {:else}
    <div>
      <button
        onclick={() => (globalDialog.dialog = unbindDialog)}
        class="{buttonClass} {shortClass} {shortColor} fd {storageStatus === 'Bound'
          ? 'fd-server'
          : 'fd-server-off'} preset-tonal-surface {iconClass} {isShutdown &&
        storageStatus === 'Bound'
          ? ''
          : 'disabled'}"
      >
      </button>
    </div>
    <Tooltip class="z-20 bg-white text-surface-950-50 dark:bg-black" placement="top">
      {#if storageStatus === 'Unbound'}
        The storage is not allocated for this pipeline, so there are no checkpoints available.
      {:else if isShutdown}
        Pipeline storage is in use. Click to delete it.
      {:else}
        The storage is used by the running pipeline. Stop the pipeline to delete it.
      {/if}
    </Tooltip>
  {/if}
{/snippet}
