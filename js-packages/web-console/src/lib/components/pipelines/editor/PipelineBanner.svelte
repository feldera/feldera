<script lang="ts">
  import { slide } from 'svelte/transition'
  import ClipboardCopyButton from '$lib/components/other/ClipboardCopyButton.svelte'

  const {
    header,
    message,
    actions = [],
    style,
    onClose
  }: {
    header: string
    message?: string
    actions?: {
      label: string
      onclick: () => void
    }[]
    onClose?: () => void
    style: 'info' | 'warning' | 'error'
  } = $props()
  let showMore = $state(false)
  let spanEl: HTMLElement | undefined = $state()
  let isTruncated = $state(false)

  // Track whether the message overflows its single visible line (line-clamp-1).
  // Skipped while expanded (showMore) to preserve isTruncated=true for "Show less".
  // ResizeObserver keeps the check in sync as the viewport width changes.
  $effect(() => {
    // eslint-disable-next-line @typescript-eslint/no-unused-expressions
    message // re-run the effect when message changes

    if (!spanEl || showMore) {
      return
    }

    const checkTruncation = () => {
      isTruncated = spanEl!.scrollHeight > spanEl!.clientHeight
    }

    const observer = new ResizeObserver(checkTruncation)
    observer.observe(spanEl)
    checkTruncation()

    return () => observer.disconnect()
  })

  const textClass = $derived(
    style === 'info'
      ? 'text-surface-950-50'
      : style === 'warning'
        ? 'text-warning-950-50'
        : 'text-error-950-50'
  )
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
        <span class={textClass}>{header}</span>
        <div class="flex flex-nowrap gap-4">
          {#if message}
            <ClipboardCopyButton value={message} class="-m-2"></ClipboardCopyButton>
          {/if}
          {#if onClose}
            <button
              onclick={onClose}
              class="fd fd-x -m-2 btn-icon text-[24px]"
              aria-label="Dismiss pipeline deployment error"
            >
            </button>
          {/if}
        </div>
      </div>
      <span
        bind:this={spanEl}
        class="{textClass} break-all whitespace-pre-wrap {showMore
          ? 'scrollbar max-h-[30vh] overflow-auto '
          : 'line-clamp-1'}"
      >
        {message}
      </span>
      {#if message && isTruncated}
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
