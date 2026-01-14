<script lang="ts">
  import PanelAdHocQuery from '$lib/components/pipelines/editor/TabAdHocQuery.svelte'
  import * as TabProfileVisualizer from '$lib/components/pipelines/editor/TabProfileVisualizer.svelte'
  import * as TabSamplyProfile from '$lib/components/pipelines/editor/TabSamplyProfile.svelte'
  import TabsPanel from '$lib/components/pipelines/editor/TabsPanel.svelte'
  import { useLayoutSettings } from '$lib/compositions/layout/useLayoutSettings.svelte'
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'
  import { tuple } from '$lib/functions/common/tuple'
  import type { PipelineMetrics } from '$lib/functions/pipelineMetrics'
  import type { ExtendedPipeline } from '$lib/services/pipelineManager'

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

  const { showInteractionPanel } = useLayoutSettings()

  const tabs = $derived([
    tuple('Ad-Hoc Queries' as const, TabControlAdhoc, PanelAdHocQuery, false),
    tuple(
      'Profile Visualizer' as const,
      TabProfileVisualizer.Label,
      TabProfileVisualizer.default,
      true
    ),
    tuple('Samply' as const, TabSamplyProfile.Label, TabSamplyProfile.default, false)
  ])

  const currentTab = $derived(
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

<TabsPanel {tabs} bind:currentTab={currentTab.value} tabProps={{ pipeline }}>
  {#snippet tabBarEnd()}
    <button
      class="fd fd-x ml-auto btn-icon text-[24px]"
      onclick={() => {
        _currentTab = null
        showInteractionPanel.value = false
      }}
      aria-label="Close"
    ></button>
  {/snippet}
</TabsPanel>
