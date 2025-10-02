<script lang="ts">
  import { page } from '$app/state'
  import PipelineEditLayout from '$lib/components/layout/pipelines/PipelineEditLayout.svelte'
  import {
    writablePipeline,
    useRefreshPipeline
  } from '$lib/compositions/useWritablePipeline.svelte.js'
  import { goto } from '$app/navigation'
  import { base } from '$app/paths'
  import type { ExtendedPipeline, PipelineThumb } from '$lib/services/pipelineManager.js'
  import { usePipelineList } from '$lib/compositions/pipelines/usePipelineList.svelte.js'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte.js'

  let { data } = $props()

  let pipelineName = $state(decodeURIComponent(page.params.pipelineName))
  $effect(() => {
    pipelineName = decodeURIComponent(page.params.pipelineName)
  })

  let pipelineCache = $state({ current: data.preloadedPipeline })
  let set = (pipeline: ExtendedPipeline) => {
    pipelineCache.current = pipeline
  }
  let update = (pipeline: Partial<ExtendedPipeline>) => {
    pipelineCache.current = { ...pipelineCache.current, ...pipeline }
  }
  const api = usePipelineManager()
  let pipeline = $derived(writablePipeline({ api, pipeline: pipelineCache, set, update }))
  const pipelineList = usePipelineList(data.preloaded)

  useRefreshPipeline({
    getPreloaded: () => data.preloadedPipeline,
    getPipelines: () => pipelineList.pipelines,
    getPipeline: () => pipelineCache,
    set,
    update,
    onNotFound: () => goto(`${base}/`)
  })
</script>

<PipelineEditLayout preloaded={data.preloaded} {pipeline}></PipelineEditLayout>
