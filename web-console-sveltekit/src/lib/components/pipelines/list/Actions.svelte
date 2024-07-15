<script lang="ts">
  import {
    getFullPipeline,
    getPipelineStatus,
    patchPipeline,
    pipelineAction,
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

  let {
    name: pipelineName,
    status,
    reload,
    class: _class = ''
  }: {
    name: string
    status: PipelineStatus
    reload?: () => void
    class?: string
  } = $props()
  $effect(() => {
    let interval = setInterval(() => reload?.(), 2000)
    return () => {
      clearInterval(interval)
    }
  })

  const globalDialog = useGlobalDialog()
  const deletePipeline = (pipelineName: string) => {
    _deletePipeline(pipelineName)
    reload?.()
  }

  const actions = {
    _start,
    _pause,
    _shutdown,
    _delete,
    _spacer,
    _spinner,
    _configure
  }

  const active = $derived(
    match(status)
      .returnType<(keyof typeof actions)[]>()
      .with('Shutdown', () => ['_start', '_configure', '_delete'])
      .with('Queued', () => ['_spacer', '_configure', '_delete'])
      .with('Starting up', () => ['_spinner', '_configure', '_spacer'])
      .with('Initializing', () => ['_spinner', '_configure'])
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

  const buttonClass = 'btn-icon preset-tonal-surface text-[24px]'
  const _reload = () => {
    setTimeout(() => reload?.(), 300)
    setTimeout(() => reload?.(), 500)
  }
</script>

{#snippet deleteDialog()}
  <DeleteDialog
    {...deleteDialogProps('Delete', (name) => `${name} pipeline`, deletePipeline)(pipelineName)}
    onClose={() => (globalDialog.dialog = null)}></DeleteDialog>
{/snippet}

{#snippet pipelineResourcesDialog()}
  {#await getFullPipeline(pipelineName) then pipeline}
    <JSONDialog
      json={JSONbig.stringify(pipeline.config, undefined, '  ')}
      onApply={async (json) => {
        await patchPipeline(pipeline.name, {
          config: JSONbig.parse(json),
          name: pipeline.name,
          description: pipeline.description
        })
      }}
      onClose={() => (globalDialog.dialog = null)}>
      {#snippet title()}
        <div class="h5 text-center font-normal">
          {`Configure ${pipelineName} runtime resources`}
        </div>
      {/snippet}
    </JSONDialog>
  {/await}
{/snippet}

<div class={'flex flex-nowrap ' + _class}>
  {#each active as name}
    {@render actions[name]()}
  {/each}
</div>

{#snippet _delete()}
  <button
    class={'bx bx-trash-alt ' + buttonClass}
    onclick={() => (globalDialog.dialog = deleteDialog)}>
  </button>
{/snippet}
{#snippet _start()}
  <button
    class={'bx bx-play-circle ' + buttonClass}
    onclick={() => pipelineAction(pipelineName, 'start').then(_reload)}>
  </button>
{/snippet}
{#snippet _pause()}
  <button
    class={'bx bx-pause-circle ' + buttonClass}
    onclick={() => pipelineAction(pipelineName, 'pause').then(_reload)}>
  </button>
{/snippet}
{#snippet _shutdown()}
  <button
    class={'bx bx-stop-circle ' + buttonClass}
    onclick={() => pipelineAction(pipelineName, 'shutdown').then(_reload)}>
  </button>
{/snippet}
{#snippet _configure()}
  <button
    onclick={() => (globalDialog.dialog = pipelineResourcesDialog)}
    class={'bx bx-cog ' + buttonClass}>
  </button>
{/snippet}
{#snippet _spacer()}
  <div class="w-9"></div>
{/snippet}
{#snippet _spinner()}
  <div class={'pointer-events-none h-9 ' + buttonClass}>
    <div class="bx bx-loader-alt btn-icon animate-spin"></div>
  </div>
{/snippet}
