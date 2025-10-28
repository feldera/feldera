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

  let {
    getPipeline,
    set,
    update
  }: {
    getPipeline: () => { current: ExtendedPipeline }
    set: (p: ExtendedPipeline) => void
    update: (p: Partial<ExtendedPipeline>) => void
  } = $props()

  // let pipelineCache: { current: ExtendedPipeline | undefined } = $state({ current: undefined })

  // const api = usePipelineManager()
  const pipelineList = usePipelineList()

  useRefreshPipeline({
    getPipelines: () => pipelineList.pipelines,
    getPipeline,
    set,
    update,
    onNotFound: () => goto(`${base}/`)
  })
</script>
