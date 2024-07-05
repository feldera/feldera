<script lang="ts">
  import { SplitPane } from '@rich_harris/svelte-split-pane'
  import { page } from '$app/stores'
  import { onMount } from 'svelte'
  import { writablePipeline } from '$lib/compositions/pipelineManager'
  import { asyncWritable, derived } from '@square/svelte-store'
  // import { useDebounce } from '$lib/compositions/debounce.svelte'
  import { useDebounce } from 'runed'
  import MonacoEditor from 'svelte-monaco'
  import { pipelineTabEq, useOpenPipelines } from '$lib/compositions/useOpenPipelines'
  import type { Pipeline } from '$lib/services/manager'
  import PipelineEditLayout from '$lib/components/layout/pipelines/PipelineEditLayout.svelte'
  import { asyncDebounced } from '$lib/compositions/asyncDebounced'
  import { useSqlErrors } from '$lib/compositions/health/systemErrors'

  let pipelineName = derived(page, (page) => decodeURI(page.params.pipelineName))
  {
    let openPipelines = useOpenPipelines()
    const addOpenedTab = (pipelineName: string) => {
      if (!openPipelines.value.find((p) => pipelineTabEq(p, { existing: pipelineName }))) {
        openPipelines.value = [...openPipelines.value, { existing: pipelineName }]
      }
    }

    onMount(() => pipelineName.subscribe(addOpenedTab))
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
  const pipelineCodeStore = asyncWritable(
    pipeline!,
    (pipeline) => pipeline.code,
    async (newCode, pipeline, oldCode) => {
      if (!pipeline || !newCode) {
        return oldCode
      }
      $pipeline = {
        ...pipeline,
        code: newCode
      }
      return newCode
    }
  )

  const errors = useSqlErrors(pipelineName)
</script>

<PipelineEditLayout {pipelineName} {pipelineCodeStore} {errors}></PipelineEditLayout>
