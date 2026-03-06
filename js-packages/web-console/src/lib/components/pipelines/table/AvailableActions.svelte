<script lang="ts">
  import { join } from 'array-join'
  import { match, P } from 'ts-pattern'
  import DeleteDialog, { deleteDialogProps } from '$lib/components/dialogs/DeleteDialog.svelte'
  import { useGlobalDialog } from '$lib/compositions/layout/useGlobalDialog.svelte'
  import { useUpdatePipelineList } from '$lib/compositions/pipelines/usePipelineList.svelte'
  import { getPipelineAction } from '$lib/compositions/usePipelineAction.svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import { usePremiumFeatures } from '$lib/compositions/usePremiumFeatures.svelte'
  import { useToast } from '$lib/compositions/useToastNotification'
  import { isPipelineCodeEditable } from '$lib/functions/pipelines/status'
  import type { PipelineThumb } from '$lib/services/pipelineManager'

  let {
    pipelines,
    selectedPipelines = $bindable()
  }: { pipelines: PipelineThumb[]; selectedPipelines: string[] } = $props()
  const { updatePipeline } = useUpdatePipelineList()
  const sortedSelectedPipelines = $derived([...selectedPipelines].sort())
  const availableActions = [
    'start' as const,
    'resume' as const,
    'pause' as const,
    'stop' as const,
    'kill' as const,
    'delete' as const,
    'clear' as const
  ]
  const isPremium = usePremiumFeatures()
  const stop = isPremium.value ? ['stop' as const] : []
  const statusActions = ({ status, storageStatus }: (typeof selected)[number]) => {
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
      .with('Paused', () => [...stop, 'kill', 'resume'])
      .with('Suspending', () => ['kill', 'delete'])
      .with('Suspended', () => ['kill', 'delete'])
      .with('Standby', () => ['kill', 'delete'])
      .with('Bootstrapping', () => ['kill', 'delete'])
      .with('Replaying', () => ['kill', 'delete'])
      .with('AwaitingApproval', () => ['kill', 'delete'])
      .with('Resuming', () => [...stop, 'kill', 'delete'])
      .with('Stopping', () => ['kill'])
      .with('Unavailable', () => [...stop, 'kill', 'delete'])
      .with('SqlError', 'RustError', 'SystemError', () => [...storageAction, 'delete'])
      .exhaustive()
  }
  const selected = $derived(
    join(
      selectedPipelines,
      pipelines,
      (name) => name,
      (pipeline) => pipeline.name,
      (_, p) => p
    )
  )
  const eligibleFor = (action: (typeof availableActions)[number]) =>
    selected
      .filter((pipeline) => statusActions(pipeline).includes(action))
      .map((p) => p.name)
      .sort()
  const actions = $derived.by(() => {
    if (selected.length === 0) return []
    const supportedByAny = new Set(selected.flatMap(statusActions))
    return availableActions
      .filter((action) => supportedByAny.has(action))
      .map((action) =>
        match(action)
          .with('start', () => btnStart)
          .with('resume', () => btnResume)
          .with('pause', () => btnPause)
          .with('stop', () => btnStop)
          .with('kill', () => btnKill)
          .with('delete', () => btnDelete)
          .with('clear', () => btnClear)
          .exhaustive()
      )
  })

  const globalDialog = useGlobalDialog()
  const api = usePipelineManager()
  const postPipelinesAction = (action: Exclude<(typeof availableActions)[number], 'delete'>) => {
    selected
      .filter((pipeline) => statusActions(pipeline).includes(action))
      .forEach((pipeline) => {
        api.postPipelineAction(pipeline.name, action)
      })
    selectedPipelines = []
  }
  const { toastError } = useToast()
  const { postPipelineAction } = getPipelineAction()
  const deletePipelines = () => {
    selected.forEach(async (pipeline) => {
      if (!isPipelineCodeEditable(pipeline.status)) {
        const { waitFor } = await postPipelineAction(
          pipeline.name,
          isPremium.value ? 'stop' : 'kill'
        )
        updatePipeline(pipeline.name, (p) => ({
          ...p,
          status: isPremium.value ? 'Stopping' : 'Stopping'
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
{#snippet btnResume()}
  <button class="btn preset-tonal-surface" onclick={() => postPipelinesAction('resume')}>
    <span class="fd fd-play text-[20px]"></span>
    Resume
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

<div class="flex h-9 flex-wrap gap-2">
  {#each actions as action}
    {@render action()}
  {/each}
</div>

{#snippet deleteDialog()}
  <DeleteDialog
    {...deleteDialogProps(
      'Delete',
      () =>
        selectedPipelines.length === 1
          ? 'You are about to delete 1 pipeline:'
          : 'You are about to delete ' + selectedPipelines.length.toFixed() + ' pipelines:',
      deletePipelines,
      'Are you sure? You will lose the associated code and computation state.\nThis action is irreversible.',
      sortedSelectedPipelines.join('\n')
    )()}
    onClose={() => (globalDialog.dialog = null)}
  ></DeleteDialog>
{/snippet}

{#snippet clearDialog()}
  {@const pipelines = eligibleFor('clear')}
  <DeleteDialog
    {...deleteDialogProps(
      'Clear',
      () =>
        pipelines.length === 1
          ? 'You are about to clear storage of 1 pipeline:'
          : 'You are about to clear storage of ' + pipelines.length.toFixed() + ' pipelines:',
      () => postPipelinesAction('clear'),
      pipelines.length === 1
        ? 'This will delete any checkpoints of this pipeline.'
        : 'This will delete any checkpoints of these pipelines.',
      pipelines.join('\n')
    )()}
    onClose={() => (globalDialog.dialog = null)}
  ></DeleteDialog>
{/snippet}

{#snippet stopDialog()}
  {@const pipelines = eligibleFor('stop')}
  <DeleteDialog
    {...deleteDialogProps(
      'Stop',
      () =>
        pipelines.length === 1
          ? 'You are about to stop 1 pipeline:'
          : 'You are about to stop ' + pipelines.length.toFixed() + ' pipelines:',
      () => postPipelinesAction('stop'),
      pipelines.length === 1
        ? 'The pipeline will stop processing inputs and make a checkpoint of its state.'
        : 'These pipelines will stop processing inputs and make checkpoints of their states.',
      pipelines.join('\n')
    )()}
    onClose={() => (globalDialog.dialog = null)}
  ></DeleteDialog>
{/snippet}

{#snippet killDialog()}
  {@const pipelines = eligibleFor('kill')}
  <DeleteDialog
    {...deleteDialogProps(
      'Force stop',
      () =>
        pipelines.length === 1
          ? 'You are about to forcefully stop 1 pipeline:'
          : 'You are about to forcefully stop ' + pipelines.length.toFixed() + ' pipelines:',
      () => postPipelinesAction('kill'),
      pipelines.length === 1
        ? 'The pipeline will stop processing inputs without making a checkpoint, leaving only a previous one, if any.'
        : 'These pipelines will stop processing inputs without making checkpoints, leaving only previous ones, if any.',
      pipelines.join('\n')
    )()}
    onClose={() => (globalDialog.dialog = null)}
  ></DeleteDialog>
{/snippet}
