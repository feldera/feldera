<script lang="ts">
  import type { PipelineTab } from '$lib/compositions/useOpenPipelines'
  import { writablePipelineName } from '$lib/compositions/writablePipelineName'
  import PipelineTabControl from './PipelineTabControl.svelte'
  import type { Snippet } from 'svelte'
  import { writableNewPipeline } from '$lib/compositions/useNewPipeline'

  let {
    new: _new,
    tab,
    currentTab,
    text,
    renamePipelineTab,
    close
  } = $props<{
    new: string
    tab: PipelineTab
    currentTab: PipelineTab
    text: Snippet
    close: { href: string; onclick: () => void } | undefined
    renamePipelineTab: (oldTab: PipelineTab, newTab: PipelineTab) => void
  }>()

  let store = writablePipelineName(writableNewPipeline(), renamePipelineTab)
</script>

<PipelineTabControl {tab} {currentTab} href={undefined} {text} bind:value={$store} {close}
></PipelineTabControl>
