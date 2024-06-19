<script lang="ts">
  import { Tabs } from '@skeletonlabs/skeleton-svelte'
  import DoubleClickInput from '$lib/components/input/DoubleClickInput.svelte'
  import type { Snippet } from 'svelte'
  import type { LayoutData } from './$types'
  import { localStore } from '$lib/compositions/localStore.svelte'
  import { asyncWritable, derived } from '@square/svelte-store'
  import { getPipeline, updatePipeline, type UpdatePipelineRequest } from '$lib/services/manager'
  import { handled } from '$lib/functions/request'
  import { writablePipeline } from '$lib/compositions/pipelineManager'
  import { page } from '$app/stores'
  import { readable } from 'svelte/store'

  let openPipelines = localStore<string[]>('pipelines/open', [])
  let { children } = $props<{ children: Snippet }>()

  const dropOpenPipeline = (pipelineName: string) =>
    openPipelines.value.splice(
      openPipelines.value.findIndex((name) => name === pipelineName),
      1
    )
  const renameOpenPipeline = (oldPipelineName: string, newPipelineName: string) => {
    openPipelines.value.splice(
      openPipelines.value.findIndex((name) => name === oldPipelineName),
      1,
      newPipelineName
    )
  }
  const pipelineName = derived(page, (page) => page.params.pipelineName)
  const pipeline = writablePipeline(pipelineName)
  const pipelineNameStore = asyncWritable(
    pipeline!,
    (pipeline) => pipeline.descriptor.name,
    async (newPipelineName, pipeline, oldPipelineName) => {
      if (newPipelineName === '' || !pipeline || !oldPipelineName) {
        return oldPipelineName
      }
      $pipeline = {
        ...pipeline,
        descriptor: { ...pipeline.descriptor, name: newPipelineName }
      }
      renameOpenPipeline(oldPipelineName, newPipelineName)
      window.location.replace('/pipelines/' + newPipelineName)
      return newPipelineName
    }
  )
</script>

<div class="flex">
  <a class="preset-grayout-surface mt-1" href="/pipelines">
    <Tabs.Control
      group={$pipelineName}
      name={undefined!}
      contentClasses="group-hover:preset-tonal-surface">
      pipelines
    </Tabs.Control>
  </a>
  {#each openPipelines.value as openPipeline}
    {#if $pipelineName === openPipeline}
      <Tabs.Control
        group={$pipelineName}
        name={openPipeline}
        contentClasses="group-hover:preset-tonal-surface">
        <DoubleClickInput bind:value={$pipelineNameStore}>
          {openPipeline}
          <a
            href={'/pipelines/' +
              (openPipelines.value.findLast((name) => name !== $pipelineName) || '')}
            onclick={() => dropOpenPipeline($pipelineName)}
            class="preset-grayout-surface btn-icon bx bx-x h-6 text-[24px]">
          </a>
        </DoubleClickInput>
      </Tabs.Control>
    {:else}
      <a
        href={$pipelineName === openPipeline ? undefined : '/pipelines/' + encodeURI(openPipeline)}>
        <Tabs.Control
          group={$pipelineName}
          name={openPipeline}
          contentClasses="group-hover:preset-tonal-surface">
          <DoubleClickInput value={openPipeline}>
            {openPipeline}
            <span class="preset-grayout-surface btn-icon bx bx-x h-6 text-[24px]"> </span>
          </DoubleClickInput>
        </Tabs.Control></a>
    {/if}
  {/each}
</div>
{@render children()}
