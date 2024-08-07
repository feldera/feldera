<script lang="ts">
  import type { PipelineTab } from '$lib/compositions/useOpenPipelines'
  import { writablePipelineName } from '$lib/compositions/writablePipelineName'
  import PipelineTabControl from './PipelineTabControl.svelte'
  import type { Snippet } from 'svelte'
  import { writableNewPipeline } from '$lib/compositions/useNewPipeline'
  import { base } from '$app/paths'

  let {
    new: _new,
    text,
    onRenamePipeline,
    close
  } = $props<{
    new: string
    text: Snippet
    close: { href: string; onclick: () => void } | undefined
    onRenamePipeline?: (oldTab: PipelineTab, newTab: PipelineTab) => void
  }>()

  let store = writablePipelineName(writableNewPipeline(), onRenamePipeline)
  $effect(() => {
    if (!$store) {
      return
    }
    const currentUrl = window.location.pathname
    const newUrl = `${base}/pipelines/${$store}/`
    if (newUrl === currentUrl) {
      return
    }
    window.location.replace(newUrl)
  })
</script>

<PipelineTabControl {text} bind:value={$store} {close}></PipelineTabControl>
