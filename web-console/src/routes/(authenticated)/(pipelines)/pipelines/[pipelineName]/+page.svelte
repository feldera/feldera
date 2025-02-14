<script lang="ts">
  import { page } from '$app/state'
  import PipelineEditLayout from '$lib/components/layout/pipelines/PipelineEditLayout.svelte'
  import {
    writablePipeline,
    useRefreshPipeline
  } from '$lib/compositions/useWritablePipeline.svelte.js'
  import { goto } from '$app/navigation'
  import { base } from '$app/paths'
  import type { ExtendedPipeline } from '$lib/services/pipelineManager.js'

  let { data } = $props()

  let pipelineName = $state(decodeURIComponent(page.params.pipelineName))
  $effect(() => {
    pipelineName = decodeURIComponent(page.params.pipelineName)
  })

  let pipelineCache = $state({ current: data.preloadedPipeline })
  let set = (pipeline: ExtendedPipeline) => {
    pipelineCache.current = pipeline
  }
  let pipeline = $derived(writablePipeline(pipelineCache, set))

  useRefreshPipeline(
    () => pipelineCache,
    set,
    () => data.preloadedPipeline,
    () => goto(`${base}/`)
  )
</script>

<PipelineEditLayout preloaded={data.preloaded} {pipeline}></PipelineEditLayout>
