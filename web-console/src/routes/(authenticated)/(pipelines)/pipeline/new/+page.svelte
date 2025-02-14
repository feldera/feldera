<script lang="ts">
  import PipelineEditLayout from '$lib/components/layout/pipelines/PipelineEditLayout.svelte'
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
</script>

<PipelineEditLayout {pipeline} status={undefined}></PipelineEditLayout>
