<script lang="ts">
  import { page } from '$app/state'
  import type { Snippet } from 'svelte'
  import type { LayoutData, LayoutProps } from './$types'
  import { usePipelineList } from '$lib/compositions/pipelines/usePipelineList.svelte'
  import OverlayDrawer from '$lib/components/layout/OverlayDrawer.svelte'
  import PipelineList from '$lib/components/pipelines/List.svelte'
  import { useLayoutSettings } from '$lib/compositions/layout/useLayoutSettings.svelte'
  import { useIsTablet } from '$lib/compositions/layout/useIsMobile.svelte'

  let { children, data }: { children: Snippet; data: LayoutData } & LayoutProps = $props()

  const isTablet = useIsTablet()
  const { showPipelinesPanel: leftDrawer } = useLayoutSettings()
  const pipelineList = usePipelineList(data.preloaded)
  $effect.pre(() => {
    // Refresh the pipeline list data when load function re-runs (e.g. tenant is changed)
    pipelineList.pipelines = data.preloaded.pipelines
  })
</script>

{@render children()}
{#if isTablet.current}
  <OverlayDrawer
    width="w-72"
    bind:open={leftDrawer.value}
    side="left"
    modal={true}
    class="bg-white-dark flex flex-col gap-2 pl-4 pr-1 pt-8"
  >
    <PipelineList
      pipelineName={page.params.pipelineName}
      pipelines={pipelineList.pipelines}
      onclose={() => {
        leftDrawer.value = false
      }}
      onaction={() => {
        leftDrawer.value = false
      }}
    ></PipelineList>
  </OverlayDrawer>
{/if}
