<script lang="ts" module>
  let scrollY = $state(0) // Preserve list scroll position between opening/closing of drawer and switching between between inline and modal drawer
</script>

<script lang="ts">
  import SidebarPipelineTreeView from '$lib/components/pipelines/SidebarPipelineTreeView.svelte'
  import { matchesSubstring } from '$lib/functions/common/string'
  import { type PipelineThumb } from '$lib/services/pipelineManager'

  let {
    pipelineName,
    pipelines = $bindable(),
    onclose,
    onaction
  }: {
    pipelineName: string
    pipelines: PipelineThumb[] | undefined
    onclose?: () => void
    onaction?: () => void
  } = $props()

  let nameSearch = $state('')
  const visiblePipelines = $derived(pipelines?.filter((p) => matchesSubstring(p.name, nameSearch)))
  const bindScrollY = (node: HTMLElement, val: { scrollY: number }) => {
    $effect(() => {
      node.scrollTop = scrollY
    })
    const handle = (e: any) => {
      scrollY = e.target.scrollTop
    }
    node.addEventListener('scroll', handle)
    return {
      destroy: () => removeEventListener('scroll', handle)
    }
  }
</script>

<div
  class="relative scrollbar flex flex-col gap-2 pr-2"
  style="overflow-y: overlay;"
  use:bindScrollY={{ scrollY }}>
  <div class="bg-white-dark sticky top-0 -mr-1 flex items-center gap-2 pb-2 pl-2">
    <input
      class="input h-8 min-w-0 flex-1"
      type="search"
      placeholder="Search pipelines..."
      bind:value={nameSearch} />
    <button onclick={onclose} class="fd fd-x btn-icon text-[24px]" aria-label="Close pipelines list"
    ></button>
  </div>
  {#if visiblePipelines}
    <SidebarPipelineTreeView pipelines={visiblePipelines} {pipelineName} {onaction}
    ></SidebarPipelineTreeView>
  {:else}
    {@render placeholderList()}
  {/if}
</div>

{#snippet placeholderList()}
  <div class="flex flex-col gap-2 pt-1 pr-2 pl-3">
    {#each new Array(10).fill(undefined) as _}
      <div class="flex flex-nowrap items-center justify-between gap-6 py-2.5">
        <div class="placeholder max-w-64 grow animate-pulse"></div>
        <div class="placeholder-circle size-3 min-h-0 animate-pulse"></div>
      </div>
    {/each}
  </div>
{/snippet}
