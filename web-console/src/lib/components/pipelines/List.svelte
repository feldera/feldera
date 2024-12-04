<script lang="ts" module>
  let scrollY = $state(0) // Preserve list scroll position between opening/closing of drawer and switching between between inline and modal drawer
</script>

<script lang="ts">
  import PipelineStatus from '$lib/components/pipelines/list/PipelineStatusDot.svelte'
  import { base } from '$app/paths'
  import { type PipelineThumb } from '$lib/services/pipelineManager'
  import { page } from '$app/stores'
  import { useDrawer } from '$lib/compositions/layout/useDrawer.svelte'

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

<div class="relative flex flex-col gap-2 overflow-y-auto scrollbar" use:bindScrollY={{ scrollY }}>
  {#each pipelines as pipeline}
    <a
      class="flex flex-nowrap items-center justify-between gap-2 rounded py-2 pl-4 {$page.params
        .pipelineName === pipeline.name
        ? 'bg-surface-50-950'
        : 'hover:bg-surface-50-950'}"
      onclick={() => {
        if (showDrawer.isMobileDrawer) {
          showDrawer.value = false
        }
      }}
      href={`${base}/pipelines/` + encodeURI(pipeline.name) + '/'}
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
