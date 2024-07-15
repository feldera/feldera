<script lang="ts">
  import PipelineStatus from '$lib/components/pipelines/list/Status.svelte'
  import PipelineActions from '$lib/components/pipelines/list/Actions.svelte'
  import { base } from '$app/paths'
  import type { PipelineThumb } from '$lib/services/pipelineManager'

  let { pipelines }: { pipelines: PipelineThumb[] } = $props()
</script>

<div class="flex flex-col gap-4 p-4">
  {#each pipelines as pipeline, i}
    <div class="flex flex-nowrap items-center gap-2">
      <a class="" href={`${base}/pipelines/` + encodeURI(pipeline.name) + '/'}>
        {pipeline.name}
      </a>
      <PipelineStatus class="ml-auto" {...pipeline}></PipelineStatus>
    </div>
  {/each}
  <a href="{base}/pipeline/new/"> + create pipeline </a>
</div>
