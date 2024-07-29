<script lang="ts">
  import { useWritablePipeline } from '$lib/compositions/pipelineManager'
  import type { PipelineTab } from '$lib/compositions/useOpenPipelines'
  import { writablePipelineName } from '$lib/compositions/writablePipelineName'
  import { readable } from 'svelte/store'
  import PipelineTabControl from '$lib/components/pipelines/tabs/PipelineTabControl.svelte'
  import type { Snippet } from 'svelte'
  import { useChangedPipelines } from '$lib/compositions/pipelines/useChangedPipelines.svelte'
  import { base } from '$app/paths'

  let {
    existing,
    text,
    onRenamePipeline,
    close,
    tabContentChanged
  }: {
    existing: string
    text: Snippet
    close: { href: string; onclick: () => void } | undefined
    onRenamePipeline?: (oldTab: PipelineTab, newTab: PipelineTab) => void
    tabContentChanged?: boolean
  } = $props()

  let store = $derived(
    writablePipelineName(useWritablePipeline(readable(existing)), onRenamePipeline)
  )
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

<PipelineTabControl {text} bind:value={$store} {close} {tabContentChanged}></PipelineTabControl>
