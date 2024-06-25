<script lang="ts">
  import { writablePipeline } from '$lib/compositions/pipelineManager'
  import type { PipelineTab } from '$lib/compositions/useOpenPipelines'
  import { writablePipelineName } from '$lib/compositions/writablePipelineName'
  import { readable } from 'svelte/store'
  import PipelineTabControl from '$lib/components/pipelines/tabs/PipelineTabControl.svelte'
  import type { Snippet } from 'svelte'

  let { existing, tab, currentTab, text, renamePipelineTab, close } = $props<{
    existing: string
    tab: PipelineTab
    currentTab: PipelineTab
    text: Snippet
    close: { href: string; onclick: () => void } | undefined
    renamePipelineTab: (oldTab: PipelineTab, newTab: PipelineTab) => void
  }>()

  let store = writablePipelineName(writablePipeline(readable(existing)), renamePipelineTab)
</script>

<PipelineTabControl {tab} {currentTab} href={undefined} {text} bind:value={$store} {close}
></PipelineTabControl>
