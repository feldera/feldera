<script lang="ts">
  import {
    deletePipeline,
    postPipelineAction,
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
  let {
    pipelines,
    selectedPipelines = $bindable()
  }: { pipelines: PipelineThumb[]; selectedPipelines: string[] } = $props()
  const { updatePipelines } = useUpdatePipelineList()
  const availableActions = [
    'start' as const,
    'pause' as const,
    'shutdown' as const,
    'delete' as const
  ]
  let statusActions = (status: PipelineStatus) =>
    match(status)
      .returnType<(typeof availableActions)[number][]>()
      .with('Queued', 'Compiling SQL', () => ['delete'])
      .with('SQL compiled', () => ['delete'])
      .with('Compiling binary', () => ['delete'])
      .with('Shutdown', () => ['start', 'delete'])
      .with('Initializing', () => ['shutdown', 'delete'])
      .with('Starting up', () => ['shutdown', 'delete'])
      .with('Running', () => ['shutdown', 'pause'])
      .with('Pausing', () => ['shutdown', 'delete'])
      .with('Paused', () => ['shutdown', 'start'])
      .with('Resuming', () => ['shutdown', 'delete'])
      .with('ShuttingDown', () => ['shutdown'])
      .with('Unavailable', () => ['delete'])
      .with(
        { SqlError: P.any },
        { RustError: P.any },
        { SystemError: P.any },
        { PipelineError: P.any },
        () => ['delete']
      )
      .exhaustive()
  let selected = $derived(
    join(
      selectedPipelines,
      pipelines,
      (name) => name,
      (pipeline) => pipeline.name,
      (name, p) => p.status
    )
  )
  let actions = $derived(
    selected
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
          .with('shutdown', () => btnShutdown)
          .with('delete', () => btnDelete)
          .exhaustive()
      )
  )
  const globalDialog = useGlobalDialog()
  let postPipelinesAction = (action: PipelineAction) => {
    selectedPipelines.forEach((pipelineName) => postPipelineAction(pipelineName, action))
    selectedPipelines = []
  }
  let deletePipelines = () => {
    selectedPipelines.forEach((pipelineName) => deletePipeline(pipelineName))
    updatePipelines((ps) => ps.filter((p) => !selectedPipelines.includes(p.name)))
    selectedPipelines = []
  }
</script>

{#snippet btnStart()}
  <button class="btn preset-tonal-surface" onclick={() => postPipelinesAction('start')}>
    <span class="fd fd-play_arrow text-[24px]"></span>
    Start
  </button>
{/snippet}
{#snippet btnPause()}
  <button class="btn preset-tonal-surface" onclick={() => postPipelinesAction('pause')}>
    <span class="fd fd-pause text-[24px]"></span>
    Pause
  </button>
{/snippet}
{#snippet btnShutdown()}
  <button class="btn preset-tonal-surface" onclick={() => postPipelinesAction('shutdown')}>
    <span class="fd fd-stop text-[24px]"></span>
    Shutdown
  </button>
{/snippet}
{#snippet btnDelete()}
  <button class="btn preset-tonal-surface" onclick={() => (globalDialog.dialog = deleteDialog)}>
    <span class="fd fd-delete text-[24px]"></span>
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
