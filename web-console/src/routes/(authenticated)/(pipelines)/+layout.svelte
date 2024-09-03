<script lang="ts">
  import type { Snippet } from 'svelte'
  import { page as pageStore } from '$app/stores'
  import NewPipelineTabControl from '$lib/components/pipelines/tabs/NewPipelineTabControl.svelte'
  import ExistingPipelineTabControl from '$lib/components/pipelines/tabs/ExistingPipelineTabControl.svelte'
  import { base } from '$app/paths'
  import { Store } from 'runed'

  let { children } = $props<{ children: Snippet }>()
  let page = new Store(pageStore)
  let pipeline: { new: string } | { existing: string } = $derived(
    page.current.url.pathname === `${base}/pipeline/new/`
      ? { new: 'new' }
      : { existing: page.current.params.pipelineName }
  )
</script>

<div class=" -mt-[35px] mb-2 ml-14 w-fit">
  {#if 'existing' in pipeline}
    {#snippet text()}
      {pipeline.existing}
    {/snippet}
    <ExistingPipelineTabControl
      {text}
      close={undefined}
      existing={pipeline.existing}
      tabContentChanged={false}
    ></ExistingPipelineTabControl>
  {:else}
    {#snippet text()}
      <i>new pipeline</i>
    {/snippet}
    <NewPipelineTabControl {text} close={undefined} new={pipeline.new}></NewPipelineTabControl>
  {/if}
</div>
{@render children()}
