<script lang="ts">
  import { useTryPipeline } from '$lib/compositions/pipelines/useTryPipeline'
  import type { Demo } from '$lib/services/pipelineManager'

  let { demo }: { demo: Demo | null } = $props()
  const tryPipeline = useTryPipeline()
</script>

{#if demo}
  <div class="card flex flex-col border p-4 border-surface-100-900">
    <div class="text-sm text-surface-700-300">{demo.type}</div>
    <button class="text-left" onclick={() => tryPipeline(demo)}>
      <span class="py-2 font-semibold">{demo.title}</span>
      <!-- <span class="fd fd-arrow-right inline-block w-2 text-[20px]"></span> -->
    </button>
    <span class="text-left text-surface-700-300">{demo.description}</span>
  </div>
{:else}
  <div class="card flex flex-col border p-4 border-surface-100-900">
    <div class="flex flex-col gap-1">
      <div class="placeholder w-16 animate-pulse"></div>
      <span class="placeholder w-48 animate-pulse bg-surface-700-300"></span>
      <span class="placeholder animate-pulse"></span>
      <span class="placeholder animate-pulse"></span>
      <span class="placeholder w-1/2 animate-pulse"></span>
    </div>
    <span class="text-left text-surface-700-300"></span>
  </div>
{/if}
