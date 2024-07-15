<script lang="ts">
  import { Tabs } from '@skeletonlabs/skeleton-svelte'
  import type { Snippet } from 'svelte'
  import { page as pageStore } from '$app/stores'
  import {
    pipelineTabEq,
    useOpenPipelines,
    type PipelineTab
  } from '$lib/compositions/useOpenPipelines'
  import invariant from 'tiny-invariant'
  import PipelineTabControl from '$lib/components/pipelines/tabs/PipelineTabControl.svelte'

  import NewPipelineTabControl from '$lib/components/pipelines/tabs/NewPipelineTabControl.svelte'
  import ExistingPipelineTabControl from '$lib/components/pipelines/tabs/ExistingPipelineTabControl.svelte'
  import { useChangedPipelines } from '$lib/compositions/pipelines/useChangedPipelines.svelte'
  import { base } from '$app/paths'
  import { Store } from 'runed'

  let { children } = $props<{ children: Snippet }>()
  let page = new Store(pageStore)
  let pipeline: { new: string } | { existing: string } = $derived(
    page.current.url.pathname === `${base}/pipeline/new/`
      ? { new: 'new' }
      : { existing: page.current.params.pipelineName }
  )
  const changedPipelines = useChangedPipelines()
</script>

<div class=" -mt-10 mb-4 ml-12 w-fit">
  {#if 'existing' in pipeline}
    {#snippet text()}
      {pipeline.existing}
    {/snippet}
    <ExistingPipelineTabControl
      {text}
      close={undefined}
      existing={pipeline.existing}
      tabContentChanged={changedPipelines.has(pipeline.existing)}></ExistingPipelineTabControl>
  {:else}
    {#snippet text()}
      <i>new pipeline</i>
    {/snippet}
    <NewPipelineTabControl {text} close={undefined} new={pipeline.new}></NewPipelineTabControl>
  {/if}
</div>
{@render children()}
