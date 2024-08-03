<script lang="ts">
  import { SplitPane } from '@rich_harris/svelte-split-pane'
  import { page } from '$app/stores'
  import { onMount } from 'svelte'
  import { useWritablePipeline } from '$lib/compositions/pipelineManager'
  import { asyncWritable, derived as derivedStore } from '@square/svelte-store'
  // import { useDebounce } from '$lib/compositions/debounce.svelte'
  import { Store, useDebounce } from 'runed'
  import MonacoEditor from 'svelte-monaco'
  import { pipelineTabEq, useOpenPipelines } from '$lib/compositions/useOpenPipelines'
  import PipelineEditLayout from '$lib/components/layout/pipelines/PipelineEditLayout.svelte'
  import { asyncDebounced } from '$lib/compositions/asyncDebounced'
  import { useSqlErrors } from '$lib/compositions/health/systemErrors'
  import { usePipelineStatus } from '$lib/compositions/pipelines/usePipelineStatus.svelte'

  let { data } = $props()

  let pipelineName = derivedStore(page, (page) => decodeURIComponent(page.params.pipelineName))

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

  const pipelineNameSignal = new Store(pipelineName)
  let status = usePipelineStatus(pipelineNameSignal)
</script>

<PipelineEditLayout {pipeline} bind:status {errors}></PipelineEditLayout>
