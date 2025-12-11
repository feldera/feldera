<script lang="ts">
  import { goto } from '$app/navigation'
  import PipelineEditLayout from '$lib/components/layout/pipelines/PipelineEditLayout.svelte'
  import { usePipelineList } from '$lib/compositions/pipelines/usePipelineList.svelte.js'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte.js'
  import {
    useRefreshPipeline,
    writablePipeline
  } from '$lib/compositions/useWritablePipeline.svelte.js'
  import { resolve } from '$lib/functions/svelte'
  import type { ExtendedPipeline, PipelineThumb } from '$lib/services/pipelineManager.js'

  const { data, params } = $props()

  let pipelineName = $state(decodeURIComponent(params.pipelineName))
  $effect(() => {
    pipelineName = decodeURIComponent(params.pipelineName)
  })

  let pipelineCache = $state({ current: data.preloadedPipeline })
  const set = (pipeline: ExtendedPipeline) => {
    pipelineCache.current = pipeline
  }
  const update = (pipeline: Partial<ExtendedPipeline>) => {
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
