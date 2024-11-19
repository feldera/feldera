<script lang="ts" module>
  let scrollY = $state(0) // Preserve list scroll position between opening/closing of drawer and switching between between inline and modal drawer
</script>

<script lang="ts">
  import PipelineStatus from '$lib/components/pipelines/list/PipelineStatusDot.svelte'
  import { base } from '$app/paths'
  import { postPipeline, type PipelineThumb } from '$lib/services/pipelineManager'
  import { page } from '$app/stores'
  import { useDrawer } from '$lib/compositions/layout/useDrawer.svelte'
  import PipelineNameInput from './PipelineNameInput.svelte'

  let { pipelines = $bindable() }: { pipelines: PipelineThumb[] } = $props()

  let showDrawer = useDrawer()

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
  class="relative flex h-full flex-col gap-2 overflow-y-auto scrollbar"
  use:bindScrollY={{ scrollY }}
>
  <div class="sticky top-0 px-4 py-0.5 bg-surface-50-950">
    <PipelineNameInput
      onShowInput={() => {
        showDrawer.value = true
      }}
      inputClass="input outline-none transition-none duration-0 bg-surface-50-950"
    >
      {#snippet createButton(onclick)}
        <div class="flex justify-center">
          <button class="btn mb-7 mt-auto self-end text-sm preset-filled-primary-500" {onclick}>
            CREATE NEW PIPELINE
          </button>
        </div>
      {/snippet}
      {#snippet afterInput(error)}
        {#if error}
          <div class="-mb-2 pt-2 text-error-500">{error}</div>
        {:else}
          <div class="-mb-2 pt-2 text-surface-600-400">Press Enter to create</div>
        {/if}
      {/snippet}
    </PipelineNameInput>
  </div>
  {#each pipelines as pipeline}
    <a
      class="-my-0.5 flex flex-nowrap items-center gap-2 border-2 border-transparent px-3.5 {$page
        .params.pipelineName === pipeline.name
        ? 'bg-white-black'
        : 'border-transparent hover:!bg-opacity-30 hover:bg-surface-100-900'}"
      onclick={() => {
        if (showDrawer.isMobileDrawer) {
          showDrawer.value = false
        }
      }}
      href={`${base}/pipelines/` + encodeURI(pipeline.name) + '/'}
    >
      <div class="w-full overflow-ellipsis whitespace-break-spaces py-1">
        <!-- Insert a thin whitespace to help break names containing underscore -->
        {pipeline.name.replaceAll('_', `_ `)}
      </div>
      <PipelineStatus class="ml-auto" {...pipeline}></PipelineStatus>
    </a>
  {/each}
  <span class="sticky bottom-0 mt-auto py-1 pl-4 bg-surface-50-950 text-surface-700-300"
    >{$page.data.felderaVersion}</span
  >
</div>
