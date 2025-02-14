<script lang="ts">
  import {
    postPipelineAction,
    type ExtendedPipeline,
    type Pipeline,
    type PipelineAction,
    type PipelineStatus
  } from '$lib/services/pipelineManager'
  import { match, P } from 'ts-pattern'
  import { deletePipeline as _deletePipeline } from '$lib/services/pipelineManager'
  import DeleteDialog, { deleteDialogProps } from '$lib/components/dialogs/DeleteDialog.svelte'
  import { useGlobalDialog } from '$lib/compositions/useGlobalDialog.svelte'
  import JSONDialog from '$lib/components/dialogs/JSONDialog.svelte'
  import JSONbig from 'true-json-bigint'
  import { goto } from '$app/navigation'
  import { base } from '$app/paths'
  import Tooltip from '$lib/components/common/Tooltip.svelte'
  import PipelineConfigurationsPopup from '$lib/components/layout/pipelines/PipelineConfigurationsPopup.svelte'
  import IconLoader from '$assets/icons/generic/loader-alt.svg?component'

  let {
    pipeline,
    onDeletePipeline,
    pipelineBusy,
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
    pipelineBusy: boolean
    unsavedChanges: boolean
    onActionSuccess?: (pipelineName: string, action: PipelineAction | 'start_paused_start') => void
    saveFile: () => void
    class?: string
  } = $props()

  const globalDialog = useGlobalDialog()
  const deletePipeline = async (pipelineName: string) => {
    await _deletePipeline(pipelineName)
    onDeletePipeline?.(pipelineName)
    goto(`${base}/`)
  }

  const actions = {
    _start,
    _start_paused,
    _start_error,
    _start_pending,
    _pause,
    _shutdown,
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
    const unscheduleButton =
      pipeline.current.deploymentDesiredStatus === 'Shutdown'
        ? ('_spacer_long' as const)
        : ('_unschedule' as const)
    return match(pipeline.current.status)
      .returnType<(keyof typeof actions)[]>()
      .with('Shutdown', { SqlWarning: P.any }, () => ['_spacer_long', '_start_paused'])
      .with('Starting up', 'Initializing', 'Pausing', 'Resuming', 'Unavailable', () => [
        '_shutdown',
        '_status_spinner'
      ])
      .with('Running', () => ['_shutdown', '_pause'])
      .with('Paused', () => ['_shutdown', '_start'])
      .with('ShuttingDown', () => ['_spacer_long', '_status_spinner'])
      .with({ PipelineError: P.any }, () => ['_shutdown', '_spacer_long'])
      .with('Queued', 'Compiling SQL', 'SQL compiled', 'Compiling binary', () => [
        unscheduleButton,
        '_start_pending'
      ])
      .with({ SqlError: P.any }, { RustError: P.any }, { SystemError: P.any }, () => [
        '_spacer_long',
        '_start_error'
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
        return postPipelineAction(pipelineName, 'shutdown').then(() => {
          onActionSuccess?.(pipelineName, 'shutdown')
          pipeline.optimisticUpdate({ status: 'ShuttingDown' })
        })
      },
      'The internal state of the pipeline will be reset.'
    )(pipeline.current.name)}
    onClose={() => (globalDialog.dialog = null)}
  ></DeleteDialog>
{/snippet}

<div class={'flex flex-nowrap gap-2 sm:gap-4 ' + _class}>
  {#each active as name}
    {@render actions[name]()}
  {/each}
  {@render _saveFile()}
  {@render _configurations()}
  {@render _delete()}
</div>

{#snippet _delete()}
  <div>
    <button
      class="{buttonClass} {shortClass} {shortColor} fd fd-trash-2 preset-tonal-surface {iconClass}"
      class:disabled={pipelineBusy}
      onclick={() => (globalDialog.dialog = deleteDialog)}
    >
    </button>
  </div>
  {#if pipelineBusy}
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
  action: (alt: boolean) => PipelineAction | 'start_paused_start',
  status: PipelineStatus
)}
  <div>
    <button
      aria-label={action(false)}
      class:disabled={unsavedChanges}
      class="{buttonClass} {longClass} {importantBtnColor}"
      onclick={async (e) => {
        const _action = action(e.ctrlKey || e.shiftKey || e.metaKey)
        const pipelineName = pipeline.current.name
        const success = await postPipelineAction(
          pipeline.current.name,
          _action === 'start_paused_start' ? 'start_paused' : _action
        )
        pipeline.optimisticUpdate({ status })
        await success()
        onActionSuccess?.(pipelineName, _action)
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
  {@render start('Start', (alt) => (alt ? 'start_paused' : 'start_paused_start'), 'Starting up')}
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
    onclick={() => {
      const pipelineName = pipeline.current.name
      postPipelineAction(pipelineName, 'pause').then(() => {
        onActionSuccess?.(pipelineName, 'pause')
        pipeline.optimisticUpdate({ status: 'Pausing' })
      })
    }}
  >
    <span class="fd fd-pause {iconClass}"></span>
    Pause
    <span></span>
  </button>
{/snippet}
{#snippet _shutdown()}
  <button
    class="{buttonClass} {longClass} {basicBtnColor}"
    onclick={() => {
      globalDialog.dialog = shutdownDialog
    }}
  >
    <span class="fd fd-square {iconClass}"></span>
    Shutdown
    <span></span>
  </button>
{/snippet}
{#snippet _saveFile()}
  <div>
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
    Cancel start
  </button>
  <Tooltip class="bg-white-dark z-20 whitespace-nowrap rounded text-surface-950-50" placement="top">
    The pipeline is scheduled to start automatically after compilation
  </Tooltip>
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
{#snippet _configureResources()}
  <button
    onclick={() => (globalDialog.dialog = resourcesDialog)}
    class="{buttonClass} {shortClass} {shortColor} fd fd-sliders-horizontal {basicBtnColor} {iconClass}"
  >
  </button>
  {#if pipelineBusy}
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
  {#if pipelineBusy}
    <Tooltip class="z-20 bg-white text-surface-950-50 dark:bg-black" placement="top">
      Stop the pipeline to edit configuration
    </Tooltip>
  {/if}
{/snippet}
{#snippet _configurations()}
  <PipelineConfigurationsPopup {pipeline} {pipelineBusy}></PipelineConfigurationsPopup>
{/snippet}
{#snippet _spacer_short()}
  <div class={shortClass}></div>
{/snippet}
{#snippet _spacer_long()}
  <div class={longClass}></div>
{/snippet}
{#snippet _spinner()}
  <IconLoader class="pointer-events-none h-9 w-9 animate-spin {iconClass} fill-surface-950-50"
  ></IconLoader>
{/snippet}
{#snippet _status_spinner()}
  <button class="{buttonClass} {longClass} pointer-events-none {basicBtnColor}">
    <IconLoader class="animate-spin {iconClass} fill-surface-950-50"></IconLoader>
    <span></span>
  </button>
{/snippet}
