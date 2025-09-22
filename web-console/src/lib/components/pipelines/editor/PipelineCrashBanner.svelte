<script lang="ts">
  import ClipboardCopyButton from '$lib/components/other/ClipboardCopyButton.svelte'
  import type { ErrorResponse } from '$lib/services/manager'
  import { slide } from 'svelte/transition'

  let { error, showActions = false }: { error: ErrorResponse; showActions?: boolean } = $props()
  let showMore = $state(false)
</script>

<div
  transition:slide
  class="flex max-w-[1600px] flex-nowrap gap-2 rounded-container border border-error-500 p-4"
>
  <div class=" fd fd-circle-alert text-[20px] text-error-500"></div>
  <div class="flex flex-col {showMore ? 'gap-2' : 'sm:flex-row'} w-full overflow-hidden">
    <div class=" flex flex-col {showMore ? '' : ''} ">
      <div class="flex justify-between pb-2 font-semibold">
        The last execution of the pipeline failed with the error code: {error.error_code}
        <ClipboardCopyButton value={error.message} class="-m-2"></ClipboardCopyButton>
      </div>
      <span class=" whitespace-pre-wrap {showMore ? 'max-h-[30vh] overflow-auto' : 'line-clamp-1'}">
        {error.message}
      </span>
      <button
        onclick={() => {
          showMore = !showMore
        }}
        class="text-start underline"
      >
        {#if showMore}
          Show less
        {:else}
          Show more
        {/if}
      </button>
    </div>
    {#if showActions}
      <div class="ml-auto self-end text-end {showMore ? ' -mt-4' : ''}">
        <button class="btn preset-filled-primary-500">Restart</button>
      </div>
    {/if}
  </div>
</div>
