<script lang="ts">
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'
  import type { ExtendedPipeline } from '$lib/services/pipelineManager'
  import type { PipelineMetrics } from '$lib/functions/pipelineMetrics'
  import { tuple } from '$lib/functions/common/tuple'
  import PanelAdHocQuery from '$lib/components/pipelines/editor/TabAdHocQuery.svelte'
  import PanelProfileVisualizer from '$lib/components/pipelines/editor/TabProfileVisualizer.svelte'
  import { useLayoutSettings } from '$lib/compositions/layout/useLayoutSettings.svelte'
  import TabsPanel from './TabsPanel.svelte'

  let {
    pipeline,
    metrics,
    currentTab: _currentTab = $bindable()
  }: {
    pipeline: { current: ExtendedPipeline }
    metrics: { current: PipelineMetrics }
    currentTab: string | null
  } = $props()
  const pipelineName = $derived(pipeline.current.name)

  let { showInteractionPanel } = useLayoutSettings()

  let tabs = $derived([
    tuple('Ad-Hoc Queries' as const, TabControlAdhoc, PanelAdHocQuery, false),
    tuple('Profile Visualizer' as const, TabControlProfileVisualizer, PanelProfileVisualizer, true)
  ])

  let currentTab = $derived(
    useLocalStorage<(typeof tabs)[number][0]>(
      'pipelines/' + pipelineName + '/currentInteractionTab',
      tabs[0][0]
    )
  )

  $effect(() => {
    _currentTab = currentTab.value
    return () => {
      _currentTab = null
    }
  })
</script>

{#snippet TabControlAdhoc()}
  <span class="inline sm:hidden"> Ad-Hoc </span>
  <span class="hidden sm:inline"> Ad-Hoc Queries </span>
{/snippet}

{#snippet TabControlProfileVisualizer()}
  <span class=""> Profiler </span>
{/snippet}

<TabsPanel {tabs} bind:currentTab={currentTab.value} tabProps={{ pipeline }}>
  {#snippet tabBarEnd()}
    <button
      class="fd fd-x btn btn-icon btn-icon-lg ml-auto !h-6"
      onclick={() => {
        _currentTab = null
        showInteractionPanel.value = false
      }}
      aria-label="Close"
    ></button>
  {/snippet}
</TabsPanel>
