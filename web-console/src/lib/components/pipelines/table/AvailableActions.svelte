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
  import { isPipelineEditable } from '$lib/functions/pipelines/status'
  import { useToast } from '$lib/compositions/useToastNotification'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import { usePremiumFeatures } from '$lib/compositions/usePremiumFeatures.svelte'
  let {
    pipelines,
    selectedPipelines = $bindable()
  }: { pipelines: PipelineThumb[]; selectedPipelines: string[] } = $props()
  const { updatePipelines } = useUpdatePipelineList()
  const availableActions = [
    'start' as const,
    'pause' as const,
    'suspend' as const,
    'shutdown' as const,
    'delete' as const
  ]
  let isPremium = usePremiumFeatures()
  let suspend = isPremium.value ? ['suspend' as const] : []
  let statusActions = (status: PipelineStatus) =>
    match(status)
      .returnType<(typeof availableActions)[number][]>()
      .with(
        { Queued: P.any },
        { CompilingSql: P.any },
        { SqlCompiled: P.any },
        { CompilingRust: P.any },
        (cause) => [
          ...(Object.values(cause)[0].cause === 'upgrade' ? ['shutdown' as const] : []),
          'delete'
        ]
      )
      .with('Shutdown', () => ['start', 'delete'])
      .with('Preparing', 'Provisioning', 'Initializing', () => ['shutdown', 'delete'])
      .with('Running', () => [...suspend, 'shutdown', 'pause'])
      .with('Pausing', () => [...suspend, 'shutdown', 'delete'])
      .with('Paused', () => [...suspend, 'shutdown', 'start'])
      .with('Suspending', () => ['shutdown', 'delete'])
      .with('Suspended', () => ['shutdown', 'start'])
      .with('Resuming', () => [...suspend, 'shutdown', 'delete'])
      .with('ShuttingDown', () => ['shutdown'])
      .with('Unavailable', () => [...suspend, 'shutdown', 'delete'])
      .with('SqlError', 'RustError', 'SystemError', () => ['delete'])
      .with({ PipelineError: P.any }, () => ['shutdown', 'delete'])
      .exhaustive()
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
    selected
      .map((p) => p.status)
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
          .with('suspend', () => btnSuspend)
          .with('shutdown', () => btnShutdown)
          .with('delete', () => btnDelete)
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
  let deletePipelines = () => {
    selected.forEach(async (pipeline) => {
      if (!isPipelineEditable(pipeline.status)) {
        await api
          .postPipelineAction(pipeline.name, 'shutdown')
          .then((waitFor) => waitFor().catch(toastError))
      }
      return api.deletePipeline(pipeline.name)
    })
    updatePipelines((ps) => ps.filter((p) => !selectedPipelines.includes(p.name)))
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
{#snippet btnSuspend()}
  <button class="btn preset-tonal-surface" onclick={() => (globalDialog.dialog = suspendDialog)}>
    <span class="fd fd-circle-stop text-[20px]"></span>
    Suspend
  </button>
{/snippet}
{#snippet btnShutdown()}
  <button class="btn preset-tonal-surface" onclick={() => (globalDialog.dialog = shutdownDialog)}>
    <span class="fd fd-square-power text-[20px]"></span>
    Shutdown
  </button>
{/snippet}
{#snippet btnDelete()}
  <button class="btn preset-tonal-surface" onclick={() => (globalDialog.dialog = deleteDialog)}>
    <span class="fd fd-trash-2 text-[20px]"></span>
    Delete
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
          ? '1 pipeline'
          : selectedPipelines.length.toFixed() + ' pipelines',
      deletePipelines
    )()}
    onClose={() => (globalDialog.dialog = null)}
  ></DeleteDialog>
{/snippet}

{#snippet suspendDialog()}
  <DeleteDialog
    {...deleteDialogProps(
      'Suspend',
      () =>
        selectedPipelines.length === 1
          ? '1 pipeline'
          : selectedPipelines.length.toFixed() + ' pipelines',
      () => {
        return postPipelinesAction('suspend')
      },
      selectedPipelines.length === 1
        ? "The pipeline's state will be preserved in the persistent storage, and the allocated resources will be released. The pipeline can be resumed from the preserved state, avoiding historic backfill."
        : "These pipelines' state will be preserved in the persistent storage, and the allocated resources will be released. The pipelines can be resumed from the preserved state, avoiding historic backfill."
    )()}
    onClose={() => (globalDialog.dialog = null)}
  ></DeleteDialog>
{/snippet}

{#snippet shutdownDialog()}
  <DeleteDialog
    {...deleteDialogProps(
      'Shutdown',
      () =>
        selectedPipelines.length === 1
          ? '1 pipeline'
          : selectedPipelines.length.toFixed() + ' pipelines',
      () => {
        return postPipelinesAction('shutdown')
      },
      selectedPipelines.length === 1
        ? 'The internal state of the pipeline will be reset.'
        : 'The internal state of these pipelines will be reset.'
    )()}
    onClose={() => (globalDialog.dialog = null)}
  ></DeleteDialog>
{/snippet}
