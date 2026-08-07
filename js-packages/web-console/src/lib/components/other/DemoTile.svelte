<script lang="ts">
  import { usePipelineList } from '$lib/compositions/pipelines/usePipelineList.svelte'
  import { useTryPipeline } from '$lib/compositions/pipelines/useTryPipeline'
  import { usePermission } from '$lib/compositions/usePermission.svelte'
  import type { Demo } from '$lib/services/pipelineManager'

  let { demo, placement }: { demo: Demo | null; placement?: string } = $props()
  const tryPipeline = useTryPipeline()
  const canCreate = usePermission('write:pipeline')
  const list = usePipelineList()
  // A read-only caller may open a demo only when its pipeline already exists
  // (the tile then navigates to it); creating one from a demo needs write access.
  const exists = $derived(!!demo && !!list.pipelines?.some((p) => p.name === demo.name))
  const enabled = $derived(exists || canCreate.allowed)
</script>

{#if demo}
  <div class="flex flex-col card border border-surface-100-900 p-4">
    <div class="text-sm text-surface-700-300">{demo.type}</div>
    <button
      class="text-left disabled:pointer-events-none disabled:opacity-50"
      disabled={!enabled}
      title={enabled ? undefined : 'Creating a demo pipeline needs write access'}
      onclick={() => tryPipeline(demo, placement)}
    >
      <span class="py-2 font-semibold">{demo.title}</span>
      <!-- <span class="fd fd-arrow-right inline-block w-2 text-[20px]"></span> -->
    </button>
    <span class="text-left text-surface-700-300">{demo.description}</span>
  </div>
{:else}
  <div class="flex flex-col card border border-surface-100-900 p-4">
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
