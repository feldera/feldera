<script lang="ts">
  import { goto } from '$app/navigation'
  import { resolve } from '$app/paths'
  import PipelineEditLayout from '$lib/components/layout/pipelines/PipelineEditLayout.svelte'
  import {
    usePipelineList,
    useUpdatePipelineList
  } from '$lib/compositions/pipelines/usePipelineList.svelte.js'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte.js'
  import {
    useRefreshPipeline,
    writablePipeline
  } from '$lib/compositions/useWritablePipeline.svelte.js'
  import type { ExtendedPipeline } from '$lib/services/pipelineManager.js'

  const { data, params } = $props()

  const { updatePipeline } = useUpdatePipelineList()

  let pipelineName = $state(decodeURIComponent(params.pipelineName))
  let deleted = $state(false)
  $effect(() => {
    pipelineName = decodeURIComponent(params.pipelineName)
    deleted = false
  })

  let pipelineCache: { current: ExtendedPipeline | undefined } = $state({ current: undefined })
  const set = (pipeline: ExtendedPipeline | undefined) => {
    // Update both single pipeline cache and pipeline list cache to ensure consistency
    pipelineCache.current = pipeline
    if (pipeline) {
      updatePipeline(pipeline.name, () => pipeline)
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
    set(undefined)
    const { pending, abort } = data.pipelinePreload
    pending.then(set, () => {})
    // Abort the in-flight fetch if the effect re-runs (user navigated to a
    // different pipeline) or the component unmounts. An aborted request
    // rejects, which the empty error handler above swallows, so no stale
    // `set` call can land on the wrong pipeline.
    return abort
  })

  const pipelineList = usePipelineList()
  const pipelineThumb = $derived(
    pipelineList.pipelines
      ? (pipelineList.pipelines.find((p) => p.name === pipelineName) ?? 'invalid_pipeline')
      : undefined
  )

  useRefreshPipeline({
    getPipelines: () => pipelineList.pipelines,
    getPipeline: () => pipelineCache,
    set,
    update,
    getDeleted: () => deleted,
    onNotFound: () => goto(resolve('/'))
  })
</script>

{#if pipelineThumb !== 'invalid_pipeline'}
  <PipelineEditLayout {pipelineName} {pipelineList} {pipelineThumb} {pipeline}></PipelineEditLayout>
{/if}
