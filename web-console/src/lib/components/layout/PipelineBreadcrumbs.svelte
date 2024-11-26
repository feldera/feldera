<script lang="ts">
  import Popup from '$lib/components/common/Popup.svelte'
  import { base } from '$app/paths'
  import { fade } from 'svelte/transition'
  import { usePipelineList } from '$lib/compositions/pipelines/usePipelineList.svelte'
  import type { Snippet } from 'svelte'
  import PipelineStatus from '$lib/components/pipelines/list/PipelineStatus.svelte'
  import type { PipelineThumb } from '$lib/services/pipelineManager'

  let {
    preloaded,
    breadcrumbs,
    after,
    end
  }: {
    preloaded?: { pipelines: PipelineThumb[] }
    breadcrumbs: { text: string; href: string }[]
    after?: Snippet
    end?: Snippet
  } = $props()
  const pipelineList = usePipelineList(preloaded)
</script>

<div class="flex flex-nowrap gap-1.5">
  <Popup>
    {#snippet trigger(toggle)}
      <button
        onclick={toggle}
        class="fd fd-menu btn-icon text-[24px] preset-tonal-surface"
        aria-label="Pipelines list"
      >
      </button>
    {/snippet}
    {#snippet content(close)}
      <div
        transition:fade={{ duration: 100 }}
        class="absolute left-0 z-10 flex max-h-[calc(100vh-150px)] w-[calc(100vw-100px)] max-w-[360px]"
      >
        <div
          class="bg-white-black w-full flex-col justify-end gap-0 overflow-y-auto rounded p-2 shadow-md scrollbar"
        >
          {#each pipelineList.pipelines as pipeline}
            <a
              onclick={close}
              href="{base}/pipelines/{pipeline.name}/"
              class="flex justify-between rounded p-2 hover:preset-tonal-surface"
            >
              {pipeline.name}
              <PipelineStatus status={pipeline.status} class=""></PipelineStatus>
            </a>
          {/each}
        </div>
      </div>
    {/snippet}
  </Popup>
  <div></div>
  {#each breadcrumbs as breadcrumb}
    <a
      class="h5 self-center whitespace-nowrap font-medium [&:not(:nth-last-child(2))]:text-surface-500"
      href={breadcrumb.href}>{breadcrumb.text}</a
    >
    <span class="h5 self-center font-medium last:hidden">/</span>
  {/each}
</div>
{@render after?.()}
<span class="ml-auto"></span>
{@render end?.()}
