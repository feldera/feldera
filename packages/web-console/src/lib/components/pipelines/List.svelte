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
    pipelines: PipelineThumb[]
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
  class="relative flex flex-col gap-2 pr-2 scrollbar"
  style="overflow-y: overlay;"
  use:bindScrollY={{ scrollY }}
>
  <div class="bg-white-dark sticky top-0 flex justify-between pb-2 pl-4">
    <span class="font-semibold">Pipelines</span>
    <button
      onclick={onclose}
      class="fd fd-x btn btn-icon btn-icon-lg"
      aria-label="Close pipelines list"
    ></button>
  </div>
  {#each pipelines as pipeline}
    <a
      class="flex h-9 flex-nowrap items-center justify-between gap-2 rounded py-2 pl-4 {pipelineName ===
      pipeline.name
        ? 'bg-surface-50-950'
        : 'hover:bg-surface-50-950'}"
      onclick={onaction}
      href={resolve(`/pipelines/${encodeURI(pipeline.name)}/`)}
    >
      <div class="min-w-0 overflow-hidden overflow-ellipsis whitespace-nowrap py-1">
        {pipeline.name}
      </div>
      <!-- Wrap pipeline name -->
      <!-- Insert a thin whitespace to help break names containing underscore -->
      <!--
      <div class="w-full overflow-ellipsis whitespace-break-spaces py-1">
          {pipeline.name.replaceAll('_', `_ `)}
      </div>
      -->
      <PipelineStatus {...pipeline}></PipelineStatus>
    </a>
  {/each}
</div>
