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

  let {
    pipeline,
    onDeletePipeline,
    pipelineBusy,
    unsavedChanges,
    onActionSuccess,
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
    _configureProgram,
    _configureResources
  }

  const active = $derived(
    match(pipeline.current.status)
      .returnType<(keyof typeof actions)[]>()
      .with('Shutdown', () => ['_spacer_long', '_start_paused'])
      .with('Queued', () => ['_spacer_long', '_start_pending'])
      .with('Starting up', () => ['_shutdown', '_status_spinner'])
      .with('Initializing', () => ['_shutdown', '_status_spinner'])
      .with('Running', () => ['_shutdown', '_pause'])
      .with('Pausing', () => ['_shutdown', '_status_spinner'])
      .with('Resuming', () => ['_shutdown', '_status_spinner'])
      .with('Paused', () => ['_shutdown', '_start'])
      .with('ShuttingDown', () => ['_status_spinner', '_spacer_long'])
      .with({ PipelineError: P.any }, () => ['_shutdown', '_spacer_long'])
      .with('Compiling SQL', 'SQL compiled', 'Compiling binary', () => [
        '_spacer_long',
        '_start_pending'
      ])
      .with('Unavailable', () => ['_spinner', '_shutdown', '_spacer_long'])
      .with({ SqlError: P.any }, { RustError: P.any }, { SystemError: P.any }, () => [
        '_spacer_long',
        '_start_error'
      ])
      .exhaustive()
  )

  const buttonClass = 'btn gap-0'
  const iconClass = 'text-[28px]'
  const shortClass = 'w-9'
  const longClass = 'w-32 justify-between pl-2'
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

<div class={'flex flex-nowrap gap-2 ' + _class}>
  {#each active as name}
    {@render actions[name]()}
  {/each}
</div>

{#snippet _delete()}
  <button
    class="{buttonClass} {shortClass} fd fd-delete bg-surface-50-950 {iconClass}"
    onclick={() => (globalDialog.dialog = deleteDialog)}
  >
  </button>
{/snippet}
{#snippet start(
  text: string,
  action: (alt: boolean) => PipelineAction | 'start_paused_start',
  status: PipelineStatus
)}
  <button
    aria-label={action(false)}
    class:disabled={unsavedChanges}
    class="{buttonClass} {longClass} preset-filled-surface-900-100"
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
    <span class="fd fd-play_arrow {iconClass}"></span>
    {text}
    <span></span>
  </button>
{/snippet}
{#snippet _start()}
  {@render start('Resume', () => 'start', 'Resuming')}
  {#if unsavedChanges}
    <Tooltip class="z-20 bg-white text-surface-950-50 dark:bg-black" placement="top">
      Save the pipeline before running
    </Tooltip>
  {/if}
{/snippet}
{#snippet _start_paused()}
  {@render start('Start', (alt) => (alt ? 'start_paused' : 'start_paused_start'), 'Starting up')}
  {#if unsavedChanges}
    <Tooltip class="z-20 bg-white text-surface-950-50 dark:bg-black" placement="top">
      Save the pipeline before running
    </Tooltip>
  {/if}
{/snippet}
{#snippet _start_disabled()}
  <div class="h-9">
    <button class="{buttonClass} {longClass} disabled preset-filled-surface-900-100">
      <span class="fd fd-play_arrow {iconClass}"></span>
      Start
      <span></span>
    </button>
  </div>
{/snippet}
{#snippet _start_error()}
  {@render _start_disabled()}
  <Tooltip class="z-20 bg-white text-surface-950-50 dark:bg-black" placement="top">
    Resolve errors before running
  </Tooltip>
{/snippet}
{#snippet _start_pending()}
  {@render _start_disabled()}
  <Tooltip class="z-20 bg-white text-surface-950-50 dark:bg-black" placement="top">
    Wait for compilation to complete
  </Tooltip>
{/snippet}
{#snippet _pause()}
  <button
    class="{buttonClass} {longClass} bg-surface-50-950"
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
    class="{buttonClass} {longClass} bg-surface-50-950"
    onclick={() => {
      const pipelineName = pipeline.current.name
      postPipelineAction(pipelineName, 'shutdown').then(() => {
        onActionSuccess?.(pipelineName, 'shutdown')
        pipeline.optimisticUpdate({ status: 'ShuttingDown' })
      })
    }}
  >
    <span class="fd fd-stop {iconClass}"></span>
    Shutdown
    <span></span>
  </button>
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
    class="{buttonClass} {shortClass} fd fd-settings_input_component bg-surface-50-950 {iconClass}"
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
    class="{buttonClass} {shortClass} fd fd-settings bg-surface-50-950 {iconClass}"
  >
  </button>
  {#if pipelineBusy}
    <Tooltip class="z-20 bg-white text-surface-950-50 dark:bg-black" placement="top">
      Stop the pipeline to edit configuration
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
  <div class="gc gc-loader-alt pointer-events-none h-9 w-9 animate-spin !text-[36px]"></div>
{/snippet}
{#snippet _status_spinner()}
  <button class="{buttonClass} {longClass} pointer-events-none bg-surface-50-950">
    <span class="gc gc-loader-alt animate-spin {iconClass}"></span>
    <span></span>
  </button>
{/snippet}
