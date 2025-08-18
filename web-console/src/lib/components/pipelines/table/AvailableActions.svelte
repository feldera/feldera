<script lang="ts">
  import {
    type PipelineAction,
    type PipelineStatus,
    type PipelineThumb
  } from '$lib/services/pipelineManager'
  import { join } from 'array-join'
  import { match, P } from 'ts-pattern'
  import { intersect2 } from '$lib/functions/common/array'
  import { useUpdatePipelineList } from '$lib/compositions/pipelines/usePipelineList.svelte'
  import DeleteDialog, { deleteDialogProps } from '$lib/components/dialogs/DeleteDialog.svelte'
  import { useGlobalDialog } from '$lib/compositions/useGlobalDialog.svelte'
  import { isPipelineCodeEditable } from '$lib/functions/pipelines/status'
  import { useToast } from '$lib/compositions/useToastNotification'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import { usePremiumFeatures } from '$lib/compositions/usePremiumFeatures.svelte'
  import { tuple } from '$lib/functions/common/tuple'
  import type { StorageStatus } from '$lib/services/manager'
  import { usePipelineAction } from '$lib/compositions/usePipelineAction.svelte'
  let {
    pipelines,
    selectedPipelines = $bindable()
  }: { pipelines: PipelineThumb[]; selectedPipelines: string[] } = $props()
  const { updatePipelines, updatePipeline } = useUpdatePipelineList()
  const availableActions = [
    'start' as const,
    'pause' as const,
    'stop' as const,
    'kill' as const,
    'delete' as const,
    'clear' as const
  ]
  let isPremium = usePremiumFeatures()
  let stop = isPremium.value ? ['stop' as const] : []
  let statusActions = ({ status, storageStatus }: (typeof selected)[number]) => {
    const storageAction = storageStatus === 'InUse' ? ['clear' as const] : []
    return match(status)
      .returnType<(typeof availableActions)[number][]>()
      .with(
        { Queued: P.any },
        { CompilingSql: P.any },
        { SqlCompiled: P.any },
        { CompilingRust: P.any },
        (cause) => [
          ...(Object.values(cause)[0].cause === 'upgrade' ? ['kill' as const] : []),
          ...storageAction,
          'delete'
        ]
      )
      .with('Stopped', () => ['start', ...storageAction, 'delete'])
      .with('Preparing', 'Provisioning', 'Initializing', () => ['kill', 'delete'])
      .with('Running', () => [...stop, 'kill', 'pause'])
      .with('Pausing', () => [...stop, 'kill', 'delete'])
      .with('Paused', () => [...stop, 'kill', 'start'])
      .with('Suspending', () => ['kill', 'delete'])
      .with('Resuming', () => [...stop, 'kill', 'delete'])
      .with('Stopping', () => ['kill'])
      .with('Unavailable', () => [...stop, 'kill', 'delete'])
      .with('SqlError', 'RustError', 'SystemError', () => [...storageAction, 'delete'])
      .exhaustive()
  }
  let selected = $derived(
    join(
      selectedPipelines,
      pipelines,
      (name) => name,
      (pipeline) => pipeline.name,
      (_, p) => p
    )
  )
  let actions = $derived(
    selected.length === 0
      ? []
      : selected
          .map(statusActions)
          .reduce(
            (acc, cur) =>
              intersect2(
                acc,
                cur,
                (e) => e,
                (e) => e,
                (a) => a
              ),
            availableActions
          )
          .map((action) =>
            match(action)
              .with('start', () => btnStart)
              .with('pause', () => btnPause)
              .with('stop', () => btnStop)
              .with('kill', () => btnKill)
              .with('delete', () => btnDelete)
              .with('clear', () => btnClear)
              .exhaustive()
          )
  )
  const globalDialog = useGlobalDialog()
  const api = usePipelineManager()
  let postPipelinesAction = (action: PipelineAction) => {
    selectedPipelines.forEach((pipelineName) => api.postPipelineAction(pipelineName, action))
    selectedPipelines = []
  }
  const { toastError } = useToast()
  const { postPipelineAction } = usePipelineAction()
  let deletePipelines = () => {
    selected.forEach(async (pipeline) => {
      if (!isPipelineCodeEditable(pipeline.status)) {
        const { waitFor } = await postPipelineAction(
          pipeline.name,
          isPremium.value ? 'stop' : 'kill'
        )
        updatePipeline(pipeline.name, (p) => ({
          ...p,
          status: isPremium.value ? 'Suspending' : 'Stopping'
        }))
        await waitFor().catch(toastError)
      }
      if (pipeline.storageStatus !== 'Cleared') {
        const { waitFor } = await postPipelineAction(pipeline.name, 'clear')
        updatePipeline(pipeline.name, (p) => ({ ...p, storageStatus: 'Clearing' }))
        await waitFor().catch(toastError)
      }
      return api.deletePipeline(pipeline.name)
    })
    selectedPipelines = []
  }
</script>

{#snippet btnStart()}
  <button class="btn preset-tonal-surface" onclick={() => postPipelinesAction('start')}>
    <span class="fd fd-play text-[20px]"></span>
    Start
  </button>
{/snippet}
{#snippet btnPause()}
  <button class="btn preset-tonal-surface" onclick={() => postPipelinesAction('pause')}>
    <span class="fd fd-pause text-[20px]"></span>
    Pause
  </button>
{/snippet}
{#snippet btnStop()}
  <button class="btn preset-tonal-surface" onclick={() => (globalDialog.dialog = stopDialog)}>
    <span class="fd fd-square text-[20px]"></span>
    Stop
  </button>
{/snippet}
{#snippet btnKill()}
  <button class="btn preset-tonal-surface" onclick={() => (globalDialog.dialog = killDialog)}>
    <span class="fd fd-square-power text-[20px]"></span>
    Force Stop
  </button>
{/snippet}
{#snippet btnDelete()}
  <button class="btn preset-tonal-surface" onclick={() => (globalDialog.dialog = deleteDialog)}>
    <span class="fd fd-trash-2 text-[20px]"></span>
    Delete
  </button>
{/snippet}
{#snippet btnClear()}
  <button class="btn preset-tonal-surface" onclick={() => (globalDialog.dialog = clearDialog)}>
    <span class="fd fd-eraser text-[20px]"></span>
    Clear storage
  </button>
{/snippet}

{#each actions as action}
  {@render action()}
{/each}

{#snippet deleteDialog()}
  <DeleteDialog
    {...deleteDialogProps(
      'Delete',
      () =>
        selectedPipelines.length === 1
          ? 'You are about to delete 1 pipeline:'
          : 'You are about to delete ' + selectedPipelines.length.toFixed() + ' pipelines:',
      deletePipelines,
      selectedPipelines.join('\n') +
        '\n' +
        'Are you sure? You will lose the associated code and computation state.\nThis action is irreversible.'
    )()}
    onClose={() => (globalDialog.dialog = null)}
  ></DeleteDialog>
{/snippet}

{#snippet clearDialog()}
  <DeleteDialog
    {...deleteDialogProps(
      'Clear',
      () =>
        selectedPipelines.length === 1
          ? 'You are about to clear storage of 1 pipeline:'
          : 'You are about to clear storage of ' +
            selectedPipelines.length.toFixed() +
            ' pipelines:',
      () => postPipelinesAction('clear'),
      selectedPipelines.join('\n') +
        '\n' +
        (selectedPipelines.length === 1
          ? 'This will delete any checkpoints of this pipeline.'
          : 'This will delete any checkpoints of these pipelines.')
    )()}
    onClose={() => (globalDialog.dialog = null)}
  ></DeleteDialog>
{/snippet}

{#snippet stopDialog()}
  <DeleteDialog
    {...deleteDialogProps(
      'Stop',
      () =>
        selectedPipelines.length === 1
          ? 'You are about to stop 1 pipeline:'
          : 'You are about to stop ' + selectedPipelines.length.toFixed() + ' pipelines:',
      () => postPipelinesAction('stop'),
      selectedPipelines.join('\n') +
        '\n' +
        (selectedPipelines.length === 1
          ? 'The pipeline will stop processing inputs and make a checkpoint of its state.'
          : 'These pipelines will stop processing inputs and make checkpoints of their states.')
    )()}
    onClose={() => (globalDialog.dialog = null)}
  ></DeleteDialog>
{/snippet}

{#snippet killDialog()}
  <DeleteDialog
    {...deleteDialogProps(
      'Force stop',
      () =>
        selectedPipelines.length === 1
          ? 'You are about to forcefully stop 1 pipeline"'
          : 'You are about to forcefully stop ' +
            selectedPipelines.length.toFixed() +
            ' pipelines:',
      () => postPipelinesAction('kill'),
      selectedPipelines.join('\n') +
        '\n' +
        (selectedPipelines.length === 1
          ? 'The pipeline will stop processing inputs without making a checkpoint, leaving only a previous one, if any.'
          : 'These pipelines will stop processing inputs without making checkpoints, leaving only previous ones, if any.')
    )()}
    onClose={() => (globalDialog.dialog = null)}
  ></DeleteDialog>
{/snippet}
