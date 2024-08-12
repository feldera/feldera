<script lang="ts">
  import { page } from '$app/stores'
  import { pipelineTabEq, useOpenPipelines } from '$lib/compositions/useOpenPipelines'
  import PipelineEditLayout from '$lib/components/layout/pipelines/PipelineEditLayout.svelte'
  import { asyncDebounced } from '$lib/compositions/asyncDebounced'
  import { extractProgramError, programErrorReport } from '$lib/compositions/health/systemErrors'
  import { useWritablePipeline } from '$lib/compositions/useWritablePipeline.svelte.js'

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

  const pipeline = useWritablePipeline(() => pipelineName, data.preloadedPipeline)

  const programErrors = $derived(
    extractProgramError(programErrorReport(pipeline.pipeline))({
      name: pipelineName,
      status: pipeline.pipeline.programStatus
    })
  )
</script>

<PipelineEditLayout {pipeline} {programErrors}></PipelineEditLayout>
