<script lang="ts">
  import { page } from '$app/stores'
  import { pipelineTabEq, useOpenPipelines } from '$lib/compositions/useOpenPipelines'
  import PipelineEditLayout from '$lib/components/layout/pipelines/PipelineEditLayout.svelte'
  import { useWritablePipeline } from '$lib/compositions/useWritablePipeline.svelte.js'
  import { goto } from '$app/navigation'
  import { base } from '$app/paths'

  let { data } = $props()

  let pipelineName = $state(decodeURIComponent($page.params.pipelineName))
  $effect(() => {
    pipelineName = decodeURIComponent($page.params.pipelineName)
  })

  {
    let openPipelines = useOpenPipelines()
    const addOpenedTab = (pipelineName: string) => {
      if (!openPipelines.value.find((p) => pipelineTabEq(p, { existing: pipelineName }))) {
        openPipelines.value = [...openPipelines.value, { existing: pipelineName }]
      }
    }
    $effect(() => {
      addOpenedTab(pipelineName)
    })
  }

  const pipeline = useWritablePipeline(
    () => pipelineName,
    data.preloadedPipeline,
    () => goto(`${base}/`)
  )
</script>

<PipelineEditLayout {pipeline}></PipelineEditLayout>
