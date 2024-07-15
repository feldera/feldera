<script lang="ts">
  import { SplitPane } from '@rich_harris/svelte-split-pane'
  import { page } from '$app/stores'
  import { onMount } from 'svelte'
  import { useWritablePipeline } from '$lib/compositions/pipelineManager'
  import { asyncWritable, derived } from '@square/svelte-store'
  // import { useDebounce } from '$lib/compositions/debounce.svelte'
  import { useDebounce } from 'runed'
  import MonacoEditor from 'svelte-monaco'
  import { pipelineTabEq, useOpenPipelines } from '$lib/compositions/useOpenPipelines'
  import type { Pipeline } from '$lib/services/manager'
  import PipelineEditLayout from '$lib/components/layout/pipelines/PipelineEditLayout.svelte'
  import { asyncDebounced } from '$lib/compositions/asyncDebounced'
  import { useSqlErrors } from '$lib/compositions/health/systemErrors'
  import { usePipelineStatus } from '$lib/compositions/pipelines/usePipelineStatus.svelte'

  let { data } = $props()

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

  const pipeline = useWritablePipeline(pipelineName, data.preloadedPipeline)

  const errors = useSqlErrors(pipelineName)

  const status = usePipelineStatus(derived(pipeline, (pipeline) => pipeline.name))
</script>

<PipelineEditLayout {pipeline} status={status.current.status} {errors}></PipelineEditLayout>
