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
  import WriteablePipelineOwner from '$lib/components/pipelines/editor/WriteablePipelineOwner.svelte'

  let { data } = $props()

  let pipelineName = $state(decodeURIComponent(page.params.pipelineName))
  $effect(() => {
    pipelineName = decodeURIComponent(page.params.pipelineName)
  })

  let pipelineCache: { current: ExtendedPipeline | undefined } = $state({ current: undefined })

  let set = (pipeline: ExtendedPipeline | undefined) => {
    pipelineCache.current = pipeline
  }
  let update = (pipeline: Partial<ExtendedPipeline>) => {
    if (!pipelineCache.current) {
      return
    }
    pipelineCache.current = { ...pipelineCache.current, ...pipeline }
  }

  const api = usePipelineManager()
  let pipeline = $derived(writablePipeline({ api, pipeline: pipelineCache, set, update }))

  $effect.pre(() => {
    set(undefined)
    data.preloaded.pipeline.then(set, () => {})
  })

  const pipelineList = usePipelineList()
  const pipelineThumb = $derived(
    pipelineList.pipelines
      ? (pipelineList.pipelines.find((p) => p.name === pipelineName) ?? 'invalid_pipeline')
      : undefined
  )
</script>

{#if pipelineThumb !== 'invalid_pipeline'}
  <PipelineEditLayout {pipelineName} {pipelineList} {pipelineThumb} {pipeline}></PipelineEditLayout>
  {#if pipelineCache.current}
    <WriteablePipelineOwner
      getPipeline={() => pipelineCache as { current: ExtendedPipeline }}
      {set}
      {update}
    ></WriteablePipelineOwner>
  {/if}
{/if}
