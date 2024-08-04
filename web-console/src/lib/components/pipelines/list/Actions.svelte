<script lang="ts">
  import {
    getPipeline,
    getPipelineStatus,
    patchPipeline,
    postPipelineAction,
    type PipelineAction,
    type PipelineStatus
  } from '$lib/services/pipelineManager'
  import { asyncDerived, asyncReadable, readable, writable } from '@square/svelte-store'
  import { match, P } from 'ts-pattern'
  import { deletePipeline as _deletePipeline } from '$lib/services/pipelineManager'
  import DangerDialog from '$lib/components/dialogs/DangerDialog.svelte'
  import DeleteDialog, { deleteDialogProps } from '$lib/components/dialogs/DeleteDialog.svelte'
  import { useGlobalDialog } from '$lib/compositions/useGlobalDialog.svelte'
  import JSONDialog from '$lib/components/dialogs/JSONDialog.svelte'
  import JSONbig from 'true-json-bigint'
  import { goto } from '$app/navigation'
  import { base } from '$app/paths'
  import Tooltip from '$lib/components/common/Tooltip.svelte'

  let {
    name: pipelineName,
    status = $bindable(),
    reloadStatus,
    onDeletePipeline,
    pipelineBusy,
    unsavedChanges,
    onActionSuccess,
    class: _class = ''
  }: {
    name: string
    status: { status: PipelineStatus }
    reloadStatus?: () => void
    onDeletePipeline?: (pipelineName: string) => void
    pipelineBusy: boolean
    unsavedChanges: boolean
    onActionSuccess?: (action: PipelineAction) => void
    class?: string
  } = $props()
  $effect(() => {
    let interval = setInterval(() => reloadStatus?.(), 2000)
    return () => {
      clearInterval(interval)
    }
  })

  const globalDialog = useGlobalDialog()
  const deletePipeline = async (pipelineName: string) => {
    await _deletePipeline(pipelineName)
    reloadStatus?.()
    onDeletePipeline?.(pipelineName)
    goto(`${base}/`)
  }

  const actions = {
    _start,
    _start_paused,
    _pause,
    _shutdown,
    _delete,
    _spacer,
    _spinner,
    _configure
  }

  const active = $derived(
    match(status.status)
      .returnType<(keyof typeof actions)[]>()
      .with('Shutdown', () => ['_start_paused', '_configure', '_delete'])
      .with('Queued', () => ['_spacer', '_configure', '_delete'])
      .with('Starting up', () => ['_spinner', '_configure', '_spacer'])
      .with('Initializing', () => ['_spinner', '_configure', '_spacer'])
      .with('Running', () => ['_pause', '_configure', '_shutdown'])
      // .with('Pausing', () => ['spinner', 'edit'])
      .with('Paused', () => ['_start', '_configure', '_shutdown'])
      .with('ShuttingDown', () => ['_spinner', '_configure', '_spacer'])
      .with({ PipelineError: P.any }, () => ['_spacer', '_configure', '_shutdown'])
      .with('Compiling sql', () => ['_spinner', '_configure', '_delete'])
      .with('Compiling bin', () => ['_spinner', '_configure', '_delete'])
      .with({ SqlError: P.any }, { RustError: P.any }, { SystemError: P.any }, () => [
        '_spacer',
        '_configure',
        '_delete'
      ])
      .exhaustive()
  )

  const buttonClass = ' btn-icon h-9 w-9 preset-tonal-surface text-[36px]'
</script>

{#snippet deleteDialog()}
  <DeleteDialog
    {...deleteDialogProps('Delete', (name) => `${name} pipeline`, deletePipeline)(pipelineName)}
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
    class={'bx bx-trash-alt  ' + buttonClass}
    onclick={() => (globalDialog.dialog = deleteDialog)}
  >
  </button>
{/snippet}
{#snippet _start()}
  <div class={buttonClass}>
    <button
      class:disabled={unsavedChanges}
      class={'bx bx-play !bg-success-200-800 '}
      onclick={() =>
        postPipelineAction(pipelineName, 'start').then(() => {
          status.status = 'Starting up'
          onActionSuccess?.('start')
        })}
    >
    </button>
  </div>
  {#if unsavedChanges}
    <Tooltip class="z-20 bg-white text-surface-950-50 dark:bg-black" placement="top">
      Save the pipeline before running
    </Tooltip>
  {/if}
{/snippet}
{#snippet _start_paused()}
  <div class={buttonClass}>
    <button
      class:disabled={unsavedChanges}
      class={'bx bx-play !bg-success-200-800 '}
      onclick={() =>
        postPipelineAction(pipelineName, 'start_paused', 'await').then(() => {
          status.status = 'Starting up'
          onActionSuccess?.('start_paused')
        })}
    >
    </button>
  </div>
  {#if unsavedChanges}
    <Tooltip class="z-20 bg-white text-surface-950-50 dark:bg-black" placement="top">
      Save the pipeline before running
    </Tooltip>
  {/if}
{/snippet}
{#snippet _pause()}
  <button
    class={'bx bx-pause ' + buttonClass}
    onclick={() =>
      postPipelineAction(pipelineName, 'pause').then(() => {
        onActionSuccess?.('pause')
        status.status = 'ShuttingDown'
      })}
  >
  </button>
{/snippet}
{#snippet _shutdown()}
  <button
    class={'bx bx-stop ' + buttonClass}
    onclick={() =>
      postPipelineAction(pipelineName, 'shutdown').then(() => {
        onActionSuccess?.('shutdown')
        status.status = 'ShuttingDown'
      })}
  >
  </button>
{/snippet}
{#snippet _configure()}
  {#snippet pipelineResourcesDialog()}
    {#await getPipeline(pipelineName) then pipeline}
      <JSONDialog
        disabled={pipelineBusy}
        json={JSONbig.stringify(pipeline.runtimeConfig, undefined, '  ')}
        onApply={async (json) => {
          await patchPipeline(pipeline.name, {
            runtimeConfig: JSONbig.parse(json)
          })
        }}
        onClose={() => (globalDialog.dialog = null)}
      >
        {#snippet title()}
          <div class="h5 text-center font-normal">
            {`Configure ${pipelineName} runtime resources`}
          </div>
        {/snippet}
      </JSONDialog>
    {/await}
  {/snippet}
  <button
    onclick={() => (globalDialog.dialog = pipelineResourcesDialog)}
    class={'bx bx-cog ' + buttonClass}
  >
  </button>
  {#if pipelineBusy}
    <Tooltip class="z-20 bg-white text-surface-950-50 dark:bg-black" placement="top">
      Stop the pipeline to edit configuration
    </Tooltip>
  {/if}
{/snippet}
{#snippet _spacer()}
  <div class="w-9"></div>
{/snippet}
{#snippet _spinner()}
  <div class={'pointer-events-none h-9 ' + buttonClass}>
    <div class="bx bx-loader-alt btn-icon animate-spin"></div>
  </div>
{/snippet}
