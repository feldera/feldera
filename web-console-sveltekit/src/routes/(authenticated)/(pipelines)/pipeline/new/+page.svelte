<script lang="ts">
  import PipelineEditLayout from '$lib/components/layout/PipelineEditLayout.svelte'
  import { asyncDebounced } from '$lib/compositions/asyncDebounced'
  import { writableNewPipeline } from '$lib/compositions/useNewPipeline'
  import { useOpenPipelines } from '$lib/compositions/useOpenPipelines'
  import { asyncWritable } from '@square/svelte-store'
  import { onMount } from 'svelte'

  {
    let openPipelines = useOpenPipelines()
    onMount(() => {
      if (
        !openPipelines.value.find((p) => typeof p === 'object' && 'new' in p && p.new === 'new')
      ) {
        openPipelines.value = [...openPipelines.value, { new: 'new' }]
      }
    })
  }

  let pipeline = writableNewPipeline()
  const debouncedPipeline = asyncDebounced(pipeline)
  let pipelineCodeStore = asyncWritable(
    debouncedPipeline,
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

<PipelineEditLayout {pipelineCodeStore}></PipelineEditLayout>
