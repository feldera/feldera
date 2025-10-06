<script lang="ts">
  import ClipboardCopyButton from '$lib/components/other/ClipboardCopyButton.svelte'
  import { slide } from 'svelte/transition'

  let {
    header,
    message,
    actions = [],
    style
  }: {
    header: string
    message?: string
    actions?: {
      label: string
      onclick: () => void
    }[]
    style: 'info' | 'warning' | 'error'
  } = $props()
  let showMore = $state(false)
</script>

<div
  transition:slide
  class="flex flex-nowrap gap-2 rounded-container border {style === 'info'
    ? 'border-surface-500'
    : style === 'warning'
      ? 'border-warning-500'
      : 'border-error-500'} p-4"
>
  <div
    class=" fd fd-circle-alert text-[20px] {style === 'info'
      ? 'text-surface-500'
      : style === 'warning'
        ? 'text-warning-500'
        : 'text-error-500'}"
  ></div>
  <div class="flex flex-col {showMore ? 'gap-2' : 'sm:flex-row'} w-full overflow-hidden">
    <div class=" flex w-full flex-col">
      <div class="flex justify-between pb-2 font-semibold">
        {header}
        {#if message}
          <ClipboardCopyButton value={message} class="-m-2"></ClipboardCopyButton>
        {/if}
      </div>
      <span class=" whitespace-pre-wrap {showMore ? 'max-h-[30vh] overflow-auto' : 'line-clamp-1'}">
        {message}
      </span>
      {#if message && message.indexOf('\n') !== -1}
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
      {/if}
    </div>
    {#if actions.length}
      {#each actions as action}
        <div class="ml-auto self-end text-end {showMore ? ' -mt-4' : ''}">
          <button class="btn preset-filled-primary-500" onclick={action.onclick}
            >{action.label}</button
          >
        </div>
      {/each}
    {/if}
  </div>
</div>
