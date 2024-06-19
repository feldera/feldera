<script lang="ts">
  import { listPipelines } from '$lib/services/manager'
  import { asyncReadable } from '@square/svelte-store'
  import { handled } from '$lib/functions/request'
  let pipelines = asyncReadable([], handled(listPipelines))
</script>

<div class="flex flex-col gap-4 p-4">
  {#each $pipelines as pipeline}
    <a class="" href={'/pipelines/' + encodeURI(pipeline.descriptor.name)}>
      {pipeline.descriptor.name}
    </a>
  {/each}
</div>
