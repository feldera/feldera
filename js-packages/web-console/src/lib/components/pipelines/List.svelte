<script lang="ts" module>
  let scrollY = $state(0) // Preserve list scroll position between opening/closing of drawer and switching between between inline and modal drawer
</script>

<script lang="ts">
  import PipelineStatus from '#lib/components/pipelines/list/PipelineStatusDot.svelte'
  import PipelineSearch from '#lib/components/pipelines/PipelineSearch.svelte'
  import { matchesPipelineSearch, nameMatchesFirst } from '#lib/functions/pipelines/search.ts'
  import { resolve } from '#lib/functions/svelte.ts'
  import { type PipelineThumb } from '#lib/services/pipelineManager.ts'

  let {
    pipelineName,
    pipelines = $bindable(),
    onclose,
    onaction,
    tagsLayout = 'right'
  }: {
    pipelineName: string
    pipelines: PipelineThumb[] | undefined
    onclose?: () => void
    onaction?: () => void
    /** The place of the tag list. See `tagsLayout` in `PipelineSearch`. */
    tagsLayout?: 'right' | 'below'
  } = $props()

  let searchQuery = $state('')
  let tagFilters = $state<string[]>([])
  const visiblePipelines = $derived(
    pipelines &&
      nameMatchesFirst(
        pipelines.filter((p) => matchesPipelineSearch(p, searchQuery, tagFilters)),
        searchQuery
      )
  )
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
  <div class="bg-white-dark sticky top-0 -mr-1 flex items-start gap-2 pb-2 pl-2">
    <PipelineSearch
      pipelines={pipelines ?? []}
      bind:query={searchQuery}
      bind:tagFilters
      {tagsLayout}
      class="flex-1"
      inputClass="flex-1"
    ></PipelineSearch>
    <button onclick={onclose} class="fd fd-x btn-icon text-[20px]" aria-label="Close pipelines list"
    ></button>
  </div>
  {#if visiblePipelines}
    {#each visiblePipelines as pipeline}
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
