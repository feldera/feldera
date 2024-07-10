<script lang="ts">
  import { asyncReadable, derived, readable } from '@square/svelte-store'
  import { getPipelines } from '$lib/services/pipelineManager'
  import { onMount } from 'svelte'
  import PipelineStatus from '$lib/components/pipelines/list/Status.svelte'
  import PipelineActions from '$lib/components/pipelines/list/Actions.svelte'
  import { base } from '$app/paths'
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
    <div class="flex flex-nowrap items-center gap-2">
      <a class="" href={`${base}/pipelines/` + encodeURI(pipeline.name) + '/'}>
        {pipeline.name}
      </a>
      <PipelineStatus class="ml-auto" pipelineName={pipeline.name}></PipelineStatus>
      <PipelineActions pipelineName={pipeline.name} reloadPipelines={() => pipelines.reload?.()}
      ></PipelineActions>
    </div>
  {/each}
  <a href="{base}/pipeline/new/"> + create pipeline </a>
</div>
