<script lang="ts">
  import { goto } from '$app/navigation'
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
  import { resolve } from '$lib/functions/svelte'
  import type { ExtendedPipeline } from '$lib/services/pipelineManager.js'

  const { data, params } = $props()

  const { updatePipeline } = useUpdatePipelineList()

  let pipelineName = $state(decodeURIComponent(params.pipelineName))
  $effect(() => {
    pipelineName = decodeURIComponent(params.pipelineName)
  })

  let pipelineCache = $state({ current: data.preloadedPipeline })
  const set = (pipeline: ExtendedPipeline) => {
    // Update both single pipeline cache and pipeline list cache to ensure consistency
    pipelineCache.current = pipeline
    updatePipeline(pipeline.name, () => pipeline)
  }
  const update = (pipeline: Partial<ExtendedPipeline>) => {
    // We don't update list cache here because this is only called when list itself is updated - so we only update single pipeline cache
    pipelineCache.current = { ...pipelineCache.current, ...pipeline }
  }
  const api = usePipelineManager()
  const pipeline = $derived(writablePipeline({ api, pipeline: pipelineCache, set, update }))
  const pipelineList = usePipelineList(data.preloaded)

  useRefreshPipeline({
    getPreloaded: () => data.preloadedPipeline,
    getPipelines: () => pipelineList.pipelines,
    getPipeline: () => pipelineCache,
    set,
    update,
    onNotFound: () => goto(resolve(`/`))
  })
</script>

<PipelineEditLayout preloaded={data.preloaded} {pipeline}></PipelineEditLayout>
