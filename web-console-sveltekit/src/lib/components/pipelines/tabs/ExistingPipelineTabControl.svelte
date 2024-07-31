<script lang="ts">
  import type { PipelineTab } from '$lib/compositions/useOpenPipelines'
  import PipelineTabControl from '$lib/components/pipelines/tabs/PipelineTabControl.svelte'
  import type { Snippet } from 'svelte'
  import { base } from '$app/paths'
  import { patchPipeline } from '$lib/services/pipelineManager'
  import { goto } from '$app/navigation'

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

  let value = $state(existing)

  $effect(() => {
    if (value === existing) {
      return
    }
    const newUrl = `${base}/pipelines/${encodeURIComponent(value)}/`
    patchPipeline(existing, { name: value }).then(() => goto(newUrl, { replaceState: true }))
    onRenamePipeline?.({ existing }, { existing: value })
  })
</script>

<PipelineTabControl {text} bind:value {close} {tabContentChanged}></PipelineTabControl>
