<script lang="ts" module>
  let scrollY = $state(0) // Preserve list scroll position between opening/closing of drawer and switching between between inline and modal drawer
</script>

<script lang="ts">
  import { matchesSubstring } from 'common-lib/string'
  import PipelineStatus from '#lib/components/pipelines/list/PipelineStatusDot.svelte'
  import { resolve } from '#lib/functions/svelte.ts'
  import { type PipelineThumb } from '#lib/services/pipelineManager.ts'

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
  class="relative scrollbar flex flex-col gap-2 pr-3"
  style="overflow-y: overlay;"
  use:bindScrollY={{ scrollY }}
>
  <div class="sticky top-0 flex flex-col gap-2 bg-page pb-2">
    <!-- 40px tall, like the file tab row beside it, so the two line up. -->
    <div class="flex h-10 items-center justify-between gap-2 pr-1 pl-3">
      <span class="text-base font-medium text-surface-600-400">Pipelines</span>
      <button
        onclick={onclose}
        class="fd fd-x btn-icon btn-icon-sm text-[16px] text-surface-700-300 hover:bg-row-hover"
        aria-label="Close pipelines list"
      ></button>
    </div>
    <!-- The browser's own clear button is hidden for one that matches the app's icons. -->
    <div class="relative">
      <input
        class="input w-full pr-8 [&::-webkit-search-cancel-button]:appearance-none"
        type="search"
        placeholder="Search pipelines..."
        bind:value={nameSearch}
      />
      {#if nameSearch}
        <button
          onclick={() => (nameSearch = '')}
          class="fd fd-x absolute top-1/2 right-2 btn-icon btn-icon-sm h-5 w-5 -translate-y-1/2 text-[14px] text-surface-600-400 hover:text-surface-950-50"
          aria-label="Clear search"
        ></button>
      {/if}
    </div>
  </div>
  {#if visiblePipelines}
    {#each visiblePipelines as pipeline}
      <a
        class="flex h-9 flex-nowrap items-center justify-between gap-2 rounded py-2 pr-1 pl-3 {pipelineName ===
        pipeline.name
          ? 'bg-surface-950-50/[0.08] font-medium text-surface-950-50'
          : 'hover:bg-row-hover'}"
        aria-current={pipelineName === pipeline.name ? 'page' : undefined}
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
  <div class="flex flex-col gap-2 pt-1 pr-3 pl-3">
    {#each new Array(10).fill(undefined) as _}
      <div class="flex flex-nowrap items-center justify-between gap-6 py-2.5">
        <div class="placeholder max-w-64 grow animate-pulse"></div>
        <div class="placeholder-circle size-3 min-h-0 animate-pulse"></div>
      </div>
    {/each}
  </div>
{/snippet}
