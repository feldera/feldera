<script lang="ts">
  import { untrack } from 'svelte'
  import PipelineEditLayout from '#lib/components/layout/pipelines/PipelineEditLayout.svelte'
  import {
    usePipelineList,
    useUpdatePipelineList
  } from '#lib/compositions/pipelines/usePipelineList.svelte.ts'
  import { usePipelineManager } from '#lib/compositions/usePipelineManager.svelte.ts'
  import {
    useRefreshPipeline,
    writablePipeline
  } from '#lib/compositions/useWritablePipeline.svelte.ts'
  import type { ExtendedPipeline, PipelineThumb } from '#lib/services/pipelineManager.ts'

  const { data, params } = $props()

  const { updatePipeline, discardPendingListRefresh } = useUpdatePipelineList()

  let pipelineName = $state(decodeURIComponent(params.pipelineName))
  let deleted = $state(false)
  $effect(() => {
    pipelineName = decodeURIComponent(params.pipelineName)
    deleted = false
    lastSeenThumb = undefined
  })

  let pipelineCache: { current: ExtendedPipeline | undefined } = $state({ current: undefined })
  const set = (pipeline: ExtendedPipeline | undefined) => {
    // Update both single pipeline cache and pipeline list cache
    const previousName = pipelineCache.current?.name
    pipelineCache.current = pipeline
    if (!pipeline) {
      return
    }
    // After a rename, the list entry has the old name until the list refreshes
    updatePipeline(previousName ?? pipeline.name, () => pipeline)
    if (previousName !== undefined && previousName !== pipeline.name) {
      // A list refresh that is in flight can return the old name and overwrite the entry
      discardPendingListRefresh()
      // Set the new name before the URL changes, so the list lookup below finds the pipeline
      pipelineName = pipeline.name
    }
  }
  const update = (pipeline: Partial<ExtendedPipeline>) => {
    if (!pipelineCache.current) {
      return
    }
    // We don't update list cache here because this is only called when list itself is updated - so we only update single pipeline cache
    pipelineCache.current = { ...pipelineCache.current, ...pipeline }
  }
  const api = usePipelineManager()
  const pipeline = $derived(writablePipeline({ api, pipeline: pipelineCache, set, update }))
  $effect.pre(() => {
    const { pending, abort } = data.pipelinePreload
    // After a rename, the new URL names the cached pipeline. Keep that pipeline, so the layout
    // stays mounted.
    const isCached = untrack(
      () => pipelineCache.current?.name === decodeURIComponent(params.pipelineName)
    )
    if (!isCached) {
      set(undefined)
    }
    pending.then(set, () => {})
    // Abort the in-flight fetch if the effect re-runs (user navigated to a
    // different pipeline) or the component unmounts. An aborted request
    // rejects, which the empty error handler above swallows, so no stale
    // `set` call can land on the wrong pipeline.
    return abort
  })

  const pipelineList = usePipelineList()
  // Retain the last-seen thumb so the readonly deleted view keeps rendering
  // after the pipeline drops out of the live list. Without this fallback the
  // {#if pipelineThumb !== 'invalid_pipeline'} guard below would unmount the
  // layout the moment polling notices the deletion.
  let lastSeenThumb = $state<PipelineThumb | undefined>(undefined)
  // Current thumb from the live pipeline list (undefined while the list is
  // loading OR once the pipeline has been removed from it).
  const liveThumb = $derived(pipelineList.pipelines?.find((p) => p.name === pipelineName))
  // Snapshot the live thumb into `lastSeenThumb` so that, after the pipeline
  // is deleted and disappears from the list, the readonly view below can
  // still render the last-known metadata. Mutating state inside `$derived`
  // is forbidden in Svelte 5, so the cache write lives in a separate effect.
  $effect(() => {
    if (liveThumb) {
      lastSeenThumb = liveThumb
    }
  })
  const pipelineThumb = $derived.by(() => {
    if (!pipelineList.pipelines) {
      return undefined
    }
    if (liveThumb) {
      return liveThumb
    }
    if (deleted && lastSeenThumb && lastSeenThumb.name === pipelineName) {
      return lastSeenThumb
    }
    return 'invalid_pipeline' as const
  })

  useRefreshPipeline({
    getPipelines: () => pipelineList.pipelines,
    getPipeline: () => pipelineCache,
    set,
    update,
    getDeleted: () => deleted,
    onNotFound: () => {
      deleted = true
    }
  })
</script>

<svelte:head>
  <title>{pipelineName} | Feldera</title>
</svelte:head>

{#if pipelineThumb !== 'invalid_pipeline'}
  <PipelineEditLayout {pipelineName} {pipelineList} {pipelineThumb} {pipeline} {deleted}
  ></PipelineEditLayout>
{/if}
