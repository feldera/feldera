<script lang="ts">
  import { asyncReadable, derived, readable } from '@square/svelte-store'
  import { getPipelines } from '$lib/services/pipelineManager'
  import { onMount } from 'svelte'
  import Status from '$lib/components/pipelines/list/Status.svelte'
  import Actions from '$lib/components/pipelines/list/Actions.svelte'
  let pipelines = asyncReadable([], getPipelines, { reloadable: true })
  onMount(() => {
    let interval = setInterval(() => pipelines.reload?.(), 2000)
    return () => {
      clearInterval(interval)
    }
  })
</script>

<div class="flex flex-col gap-4 p-4">
  {#each $pipelines as pipeline, i}
    <div class="flex flex-nowrap">
      <a class="" href={'/pipelines/' + encodeURI(pipeline.name) + '/'}>
        {pipeline.name}
      </a>
      <Status pipelineName={pipeline.name}></Status>
      <Actions pipelineName={pipeline.name} reloadPipelines={() => pipelines.reload?.()}></Actions>
    </div>
  {/each}
  <a href="/pipeline/new/"> + create pipeline </a>
</div>
