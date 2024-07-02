<script lang="ts">
  import { writablePipeline } from '$lib/compositions/pipelineManager'
  import type { PipelineTab } from '$lib/compositions/useOpenPipelines'
  import { writablePipelineName } from '$lib/compositions/writablePipelineName'
  import { readable } from 'svelte/store'
  import PipelineTabControl from '$lib/components/pipelines/tabs/PipelineTabControl.svelte'
  import type { Snippet } from 'svelte'
  import { useChangedPipelines } from '$lib/compositions/pipelines/useChangedPipelines.svelte'

  let {
    existing,
    tab,
    currentTab,
    text,
    renamePipelineTab,
    close,
    tabContentChanged
  }: {
    existing: string
    tab: PipelineTab
    currentTab: PipelineTab
    text: Snippet
    close: { href: string; onclick: () => void } | undefined
    renamePipelineTab: (oldTab: PipelineTab, newTab: PipelineTab) => void
    tabContentChanged?: boolean
  } = $props()

  let store = writablePipelineName(writablePipeline(readable(existing)), renamePipelineTab)

  const changedPipelines = useChangedPipelines()
</script>

<PipelineTabControl
  {tab}
  {currentTab}
  href={undefined}
  {text}
  bind:value={$store}
  {close}
  {tabContentChanged}>
</PipelineTabControl>
