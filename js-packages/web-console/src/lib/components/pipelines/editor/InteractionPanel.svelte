<script lang="ts">
  import PanelAdHocQuery from '$lib/components/pipelines/editor/TabAdHocQuery.svelte'
  import * as TabProfileVisualizer from '$lib/components/pipelines/editor/TabProfileVisualizer.svelte'
  import * as TabSamplyProfile from '$lib/components/pipelines/editor/TabSamplyProfile.svelte'
  import TabsPanel from '$lib/components/pipelines/editor/TabsPanel.svelte'
  import { useLayoutSettings } from '$lib/compositions/layout/useLayoutSettings.svelte'
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'
  import type { PipelineMetrics } from '$lib/functions/pipelineMetrics'
  import type { ExtendedPipeline } from '$lib/services/pipelineManager'

  let {
    pipeline,
    deleted = false,
    currentTab: _currentTab = $bindable()
  }: {
    pipeline: { current: ExtendedPipeline }
    metrics: { current: PipelineMetrics }
    deleted?: boolean
    currentTab: string | null
  } = $props()
  const pipelineName = $derived(pipeline.current.name)

  const { showInteractionPanel } = useLayoutSettings()

  const tabs = $derived([
    {
      id: 'Ad-Hoc Queries' as const,
      label: TabControlAdhoc,
      panel: PanelAdHocQuery,
      keepAlive: false,
      tabBarEnd: TabBarEndClose
    },
    {
      id: TabProfileVisualizer.id,
      label: TabProfileVisualizer.Label,
      panel: TabProfileVisualizer.default,
      keepAlive: true,
      tabBarEnd: TabBarEndClose
    },
    {
      id: 'Samply' as const,
      label: TabSamplyProfile.Label,
      panel: TabSamplyProfile.default,
      keepAlive: false,
      tabBarEnd: TabBarEndClose
    }
  ])

  const currentTab = $derived(
    useLocalStorage<(typeof tabs)[number]['id']>(
      'pipelines/' + pipelineName + '/currentInteractionTab',
      tabs[0].id
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

{#snippet TabBarEndClose()}
  <button
    class="fd fd-x ml-auto btn-icon text-[24px]"
    onclick={() => {
      _currentTab = null
      showInteractionPanel.value = false
    }}
    aria-label="Close"
  ></button>
{/snippet}

<TabsPanel {tabs} bind:currentTab={currentTab.value} tabProps={{ pipeline, deleted }} />
