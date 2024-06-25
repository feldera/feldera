<script lang="ts">
  import { SplitPane } from '@rich_harris/svelte-split-pane'
  import { page } from '$app/stores'
  import { onMount } from 'svelte'
  import { writablePipeline } from '$lib/compositions/pipelineManager'
  import { asyncWritable, derived } from '@square/svelte-store'
  // import { useDebounce } from '$lib/compositions/debounce.svelte'
  import { useDebounce } from 'runed'
  import MonacoEditor from 'svelte-monaco'
  import { useOpenPipelines } from '$lib/compositions/useOpenPipelines'
  import { reactiveEq } from '$lib/functions/svelte'
  import type { Pipeline } from '$lib/services/manager'
  import PipelineEditLayout from '$lib/components/layout/PipelineEditLayout.svelte'
  import { asyncDebounced } from '$lib/compositions/asyncDebounced'

  let pipelineName = derived(page, (page) => decodeURI(page.params.pipelineName))
  {
    let openPipelines = useOpenPipelines()
    onMount(() => {
      if (!openPipelines.value.find((p) => reactiveEq(p, { existing: $pipelineName }))) {
        openPipelines.value = [...openPipelines.value, { existing: $pipelineName }]
      }
    })
  }

  const pipeline = writablePipeline(pipelineName)
  // const debounce = useDebounce((p: typeof $pipeline) => {
  //   $pipeline = p
  // }, 1000)
  // const  = asyncWritable(
  //   pipeline,
  //   (p) => p,
  //   async (p) => {
  //     debounce(p)
  //     return p
  //   }
  // )
  const debouncedPipeline = asyncDebounced(pipeline)
  const pipelineCodeStore = asyncWritable(
    pipeline!,
    (pipeline) => pipeline.code,
    async (newCode, pipeline, oldCode) => {
      if (!pipeline || !newCode) {
        return oldCode
      }
      $debouncedPipeline = {
        ...pipeline,
        code: newCode
      }
      return newCode
    }
  )
</script>

<PipelineEditLayout {pipelineName} {pipelineCodeStore}></PipelineEditLayout>
