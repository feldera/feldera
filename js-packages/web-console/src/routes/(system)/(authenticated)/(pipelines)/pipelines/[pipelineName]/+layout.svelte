<script lang="ts">
  import type { Snippet } from 'svelte'
  import OverlayDrawer from '$lib/components/layout/OverlayDrawer.svelte'
  import PipelineList from '$lib/components/pipelines/List.svelte'
  import { useIsTablet } from '$lib/compositions/layout/useIsMobile.svelte'
  import { useLayoutSettings } from '$lib/compositions/layout/useLayoutSettings.svelte'
  import { usePipelineList } from '$lib/compositions/pipelines/usePipelineList.svelte'
  import type { LayoutData, LayoutProps } from './$types'

  const { children, data, params }: { children: Snippet; data: LayoutData } & LayoutProps = $props()

  const isTablet = useIsTablet()
  const { showPipelinesPanel: leftDrawer } = useLayoutSettings()
  const pipelineList = usePipelineList(data.preloaded)
</script>

{@render children()}
{#if isTablet.current}
  <OverlayDrawer
    width="w-72"
    bind:open={leftDrawer.value}
    side="left"
    modal={true}
    class="bg-white-dark flex flex-col gap-2 pt-8 pr-1 pl-4"
  >
    <PipelineList
      pipelineName={params.pipelineName}
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
