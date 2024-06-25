<script lang="ts">
  import { Tabs } from '@skeletonlabs/skeleton-svelte'
  import type { Snippet } from 'svelte'
  import { derived } from '@square/svelte-store'
  import { page } from '$app/stores'
  import { useOpenPipelines, type PipelineTab } from '$lib/compositions/useOpenPipelines'
  import { reactiveEq } from '$lib/functions/svelte'
  import invariant from 'tiny-invariant'
  import PipelineTabControl from '$lib/components/pipelines/tabs/PipelineTabControl.svelte'

  import NewPipelineTabControl from '$lib/components/pipelines/tabs/NewPipelineTabControl.svelte'
  import ExistingPipelineTabControl from '$lib/components/pipelines/tabs/ExistingPipelineTabControl.svelte'

  let openPipelines = useOpenPipelines()
  let { children } = $props<{ children: Snippet }>()

  const dropOpenPipeline = (pipelineTab: PipelineTab) => {
    openPipelines.value.splice(
      openPipelines.value.findIndex((name) => reactiveEq(name, pipelineTab)),
      1
    )
  }
  const renamePipelineTab = (oldTab: PipelineTab, newTab: PipelineTab) => {
    const idx = openPipelines.value.findIndex((name) => reactiveEq(name, oldTab))
    if (idx === -1) {
      return
    }
    openPipelines.value.splice(idx, 1, newTab)
  }
  const currentTab = derived(page, (page) => {
    return page.url.pathname === '/pipelines/'
      ? ('pipelines' as const)
      : page.url.pathname === '/pipeline/new/'
        ? { new: 'new' }
        : { existing: page.params.pipelineName }
  })
</script>

<div class="flex">
  <a class="preset-grayout-surface" href="/pipelines/">
    <Tabs.Control
      group={JSON.stringify($currentTab)}
      name={'"pipelines"'}
      contentClasses="group-hover:preset-tonal-surface"
    >
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
    {#if reactiveEq($currentTab, openPipeline)}
      {invariant(((x): x is PipelineTab => true)($currentTab))}
      {@const close = {
        href: ((last) =>
          last
            ? 'existing' in last
              ? '/pipelines/' + last.existing + '/'
              : '/pipeline/new/'
            : '/pipelines/')(
          openPipelines.value.findLast((name) => !reactiveEq(name, $currentTab))
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
        ></ExistingPipelineTabControl>
      {:else if $currentTab.new}
        <NewPipelineTabControl
          tab={$currentTab}
          currentTab={openPipeline}
          {text}
          {close}
          new={$currentTab.new}
          {renamePipelineTab}
        ></NewPipelineTabControl>
      {/if}
    {:else}
      <PipelineTabControl
        tab={$currentTab}
        currentTab={openPipeline}
        href={'existing' in openPipeline
          ? '/pipelines/' + encodeURI(openPipeline.existing) + '/'
          : '/pipeline/new/'}
        {text}
        value={undefined}
        close={undefined}
      ></PipelineTabControl>
    {/if}
  {/each}
</div>
{@render children()}
