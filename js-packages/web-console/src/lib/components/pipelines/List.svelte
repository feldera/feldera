<script lang="ts" module>
  let scrollY = $state(0) // Preserve list scroll position between opening/closing of drawer and switching between between inline and modal drawer
</script>

<script lang="ts">
  import PipelineStatus from '$lib/components/pipelines/list/PipelineStatusDot.svelte'
  import { resolve } from '$lib/functions/svelte'
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
  use:bindScrollY={{ scrollY }}
>
  <div class="bg-white-dark sticky top-0 -mr-1 flex justify-between pb-2 pl-4">
    <span class="content-center font-semibold">Pipelines</span>
    <button onclick={onclose} class="fd fd-x btn-icon text-[24px]" aria-label="Close pipelines list"
    ></button>
  </div>
  {#if pipelines}
    {#each pipelines as pipeline}
      <a
        class="flex h-9 flex-nowrap items-center justify-between gap-2 rounded py-2 pl-4 {pipelineName ===
        pipeline.name
          ? 'bg-surface-50-950'
          : 'hover:bg-surface-50-950'}"
        onclick={onaction}
        href={resolve(`/pipelines/${encodeURI(pipeline.name)}/`)}
      >
        <div class="min-w-0 overflow-hidden py-1 overflow-ellipsis whitespace-nowrap">
          {pipeline.name}
        </div>
        <PipelineStatus {...pipeline}></PipelineStatus>
      </a>
    {/each}
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
