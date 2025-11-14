<script lang="ts">
  import Popup from '$lib/components/common/Popup.svelte'
  import { resolve } from '$lib/functions/svelte'
  import { fade } from 'svelte/transition'
  import { usePipelineList } from '$lib/compositions/pipelines/usePipelineList.svelte'
  import type { Snippet } from '$lib/types/svelte'
  import PipelineStatus from '$lib/components/pipelines/list/PipelineStatus.svelte'
  import type { PipelineThumb } from '$lib/services/pipelineManager'

  let {
    preloaded,
    trigger
  }: {
    preloaded?: { pipelines: PipelineThumb[] }
    trigger: Snippet<[() => void]>
  } = $props()
  const pipelineList = usePipelineList(preloaded)
</script>

<Popup
  {trigger}
  wrapperClass="max-w-fit w-[calc(100vw-190px)] md:w-[calc(100vw-470px)] xl:w-[calc(100vw-950px)] text-ellipsis"
>
  {#snippet content(close)}
    <div
      transition:fade={{ duration: 100 }}
      class="absolute left-0 z-10 -ml-16 flex max-h-[calc(100vh-150px)] w-[calc(100vw-50px)] max-w-[480px] sm:-ml-4"
    >
      <div
        class="bg-white-dark w-full flex-col justify-end gap-0 overflow-y-auto rounded p-2 shadow-md scrollbar"
      >
        {#each pipelineList.pipelines as pipeline}
          <a
            onclick={close}
            href={resolve(`/pipelines/${pipeline.name}/`)}
            class="flex justify-between whitespace-nowrap rounded p-2 hover:preset-tonal-surface"
          >
            <span class="min-w-0 overflow-hidden overflow-ellipsis">{pipeline.name}</span>

            <PipelineStatus status={pipeline.status} class="flex-none"></PipelineStatus>
          </a>
        {/each}
      </div>
    </div>
  {/snippet}
</Popup>
