<script lang="ts">
  import { Tabs } from '@skeletonlabs/skeleton-svelte'
  import type { Snippet } from 'svelte'
  import { derived } from '@square/svelte-store'
  import { page } from '$app/stores'
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

  let { children } = $props<{ children: Snippet }>()
  let openPipelines = useOpenPipelines()

  const dropOpenPipeline = (pipelineTab: PipelineTab) => {
    openPipelines.value.splice(
      openPipelines.value.findIndex((name) => pipelineTabEq(name, pipelineTab)),
      1
    )
  }
  const renamePipelineTab = (oldTab: PipelineTab, newTab: PipelineTab) => {
    const idx = openPipelines.value.findIndex((name) => pipelineTabEq(name, oldTab))
    if (idx === -1) {
      return
    }
    openPipelines.value.splice(idx, 1, newTab)
  }
  const currentTab = derived(page, (page) => {
    return page.url.pathname === `${base}/pipelines/`
      ? ('pipelines' as const)
      : page.url.pathname === `${base}/pipeline/new/`
        ? { new: 'new' }
        : { existing: page.params.pipelineName }
  })
  const changedPipelines = useChangedPipelines()
</script>

<div class="flex">
  <a class="preset-grayout-surface" href="{base}/pipelines/">
    <Tabs.Control
      group={JSON.stringify($currentTab)}
      name={'"pipelines"'}
      contentClasses="group-hover:preset-tonal-surface">
      pipelines
    </Tabs.Control>
  </a>
  {#each openPipelines.value as openPipeline}
    {#snippet text()}
      {#if typeof openPipeline === 'object' && 'existing' in openPipeline}
        {openPipeline.existing}
      {:else}
        <i>new pipeline</i>
      {/if}
    {/snippet}
    {#if pipelineTabEq($currentTab, openPipeline)}
      {invariant(((x): x is PipelineTab => true)($currentTab))}
      {@const close = {
        href: ((last) =>
          last
            ? 'existing' in last
              ? `${base}/pipelines/` + last.existing + '/'
              : `${base}/pipeline/new/`
            : `${base}/pipelines/`)(
          openPipelines.value.findLast((name) => !pipelineTabEq(name, $currentTab))
        ),
        onclick: () => dropOpenPipeline($currentTab)
      }}
      {#if 'existing' in $currentTab && $currentTab.existing}
        <ExistingPipelineTabControl
          tab={$currentTab}
          currentTab={openPipeline}
          {text}
          {close}
          existing={$currentTab.existing}
          {renamePipelineTab}
          tabContentChanged={changedPipelines.has($currentTab.existing)}
        ></ExistingPipelineTabControl>
      {:else if $currentTab.new}
        <NewPipelineTabControl
          tab={$currentTab}
          currentTab={openPipeline}
          {text}
          {close}
          new={$currentTab.new}
          {renamePipelineTab}></NewPipelineTabControl>
      {/if}
    {:else}
      <PipelineTabControl
        tab={$currentTab}
        currentTab={openPipeline}
        href={'existing' in openPipeline
          ? `${base}/pipelines/` + encodeURI(openPipeline.existing) + '/'
          : `${base}/pipeline/new/`}
        {text}
        value={undefined}
        close={undefined}
        tabContentChanged={'existing' in openPipeline
          ? changedPipelines.has(openPipeline.existing)
          : undefined}></PipelineTabControl>
    {/if}
  {/each}
</div>
{@render children()}
