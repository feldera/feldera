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

  let value = {
    get value() {
      return existing
    },
    set value(name: string) {
      const newUrl = `${base}/pipelines/${encodeURIComponent(name)}/`
      patchPipeline(existing, { name }).then(() => {
        onRenamePipeline?.({ existing }, { existing: name })
        goto(newUrl, { replaceState: true })
      })
    }
  }
</script>

<PipelineTabControl {text} bind:value={value.value} {close} {tabContentChanged}
></PipelineTabControl>
