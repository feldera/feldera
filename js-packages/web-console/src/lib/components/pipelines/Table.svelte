<script lang="ts">
  import PipelineTreeView from '$lib/components/pipelines/PipelineTreeView.svelte'
  import {
    usePipelineList,
    useUpdatePipelineList
  } from '$lib/compositions/pipelines/usePipelineList.svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import { dateMax } from '$lib/functions/common/date'
  import { matchesSubstring } from '$lib/functions/common/string'
  import { type NamesInUnion, unionName } from '$lib/functions/common/union'
  import { useElapsedTime } from '$lib/compositions/common/useElapsedTime'
  import { formatDateTime } from '$lib/functions/format'
  import type {
    PipelineStatus as PipelineStatusType,
    PipelineThumb
  } from '$lib/services/pipelineManager'
  import type { Snippet } from '$lib/types/svelte'

  let {
    header,
    preHeaderEnd,
    selectedPipelines = $bindable()
  }: {
    header?: Snippet
    preHeaderEnd?: Snippet
    selectedPipelines: string[]
  } = $props()

  const pipelineList = usePipelineList()
  const pipelines = $derived(pipelineList.pipelines ?? [])

  let controlsHeight = $state(0)

  const filterStatuses: [string, NamesInUnion<PipelineThumb['status']>[]][] = [
    ['All Pipelines', []],
    ['Running', ['Running']],
    ['Paused', ['Paused']],
    ['Ready To Start', ['Stopped']],
    ['Compiling', ['Queued', 'CompilingSql', 'SqlCompiled', 'CompilingRust']],
    ['Failed', ['SystemError', 'SqlError', 'RustError']]
  ]

  let nameSearch = $state('')
  let statusFilterKey = $state('All Pipelines')

  const visiblePipelines = $derived(
    pipelines
      .filter((p) => matchesSubstring(p.name, nameSearch))
      .filter((p) => {
        const allowed = filterStatuses.find((f) => f[0] === statusFilterKey)![1]
        return !allowed.length || allowed.includes(unionName(p.status))
      })
      .map((pipeline) => ({
        ...pipeline,
        lastStatusSince: dateMax(
          new Date(pipeline.deploymentStatusSince),
          new Date(pipeline.programStatusSince)
        )
      }))
  )

  // Folder path lives in the top-level `path` field of client metadata.
  const getFolderPath = (p: { path?: string | null }) => {
    return (p.path ?? '').replace(/^\/+|\/+$/g, '')
  }

  const api = usePipelineManager()
  const { updatePipeline, discardPendingListRefresh } = useUpdatePipelineList()
  const movePipeline = (name: string, newFolderPath: string) => {
    const current = pipelineList.pipelines?.find((p) => p.name === name)
    if (!current) {
      return
    }
    if (newFolderPath === (current.path ?? '')) {
      return
    }
    discardPendingListRefresh()
    updatePipeline(name, (p) => ({ ...p, path: newFolderPath }))
    api.patchPipeline(name, { path: newFolderPath })
  }
  const createFolderFor = (a: string, b: string, newFolderPath: string) => {
    movePipeline(a, newFolderPath)
    movePipeline(b, newFolderPath)
  }
  // Move a whole folder: re-path every leaf whose folder path equals or starts
  // with `oldPath`, replacing that prefix with `newPath`. Empty string = top
  // level; treated like any other path here.
  const moveFolder = (oldPath: string, newPath: string) => {
    if (oldPath === newPath) {
      return
    }
    for (const p of pipelineList.pipelines ?? []) {
      const path = getFolderPath(p)
      const matchesExact = path === oldPath
      const matchesPrefix = oldPath !== '' && path.startsWith(oldPath + '/')
      if (!matchesExact && !matchesPrefix) {
        continue
      }
      const suffix = matchesExact ? '' : path.slice(oldPath.length) // includes leading '/'
      const target = (newPath + suffix).replace(/^\/+|\/+$/g, '')
      movePipeline(p.name, target)
    }
  }
</script>

<div class="bg-white-dark">
  <div class="bg-white-dark sticky top-0 z-10 pb-2" bind:clientHeight={controlsHeight}>
    <div class="sticky left-0 max-w-[100cqi] px-2 md:px-8">
      {#if header}
        {@render header()}
      {/if}
      <div
        class="relative mt-2 flex flex-row items-stretch gap-2 sm:items-end sm:justify-end sm:gap-4"
        class:lg:-mt-7={!!header}
        class:lg:mb-0={!!header}>
        <input
          data-testid="input-pipeline-search"
          class="input h-9 sm:w-60"
          type="search"
          placeholder="Search pipelines..."
          oninput={(e) => {
            nameSearch = e.currentTarget.value
          }} />
        <select
          data-testid="select-pipeline-status"
          class="h_-9 select sm:w-40"
          bind:value={statusFilterKey}>
          {#each filterStatuses as filter (filter[0])}
            <option value={filter[0]}>{filter[0]}</option>
          {/each}
        </select>
        {@render preHeaderEnd?.()}
      </div>
    </div>
  </div>
  {#if visiblePipelines.length === 0}
    <div class="px-2 py-4 md:px-8">No pipelines found</div>
  {:else}
    <PipelineTreeView
      items={visiblePipelines}
      {getFolderPath}
      bind:selected={selectedPipelines}
      onMoveToFolder={movePipeline}
      onCreateFolderFor={createFolderFor}
      onMoveFolder={moveFolder}
      stickyTop={controlsHeight} />
  {/if}
</div>
