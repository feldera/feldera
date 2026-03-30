<script lang="ts">
  import { Tooltip } from '$lib/components/common/Tooltip.svelte'
  import {
    type GlobalDialogContent,
    useGlobalDialog
  } from '$lib/compositions/layout/useGlobalDialog.svelte'
  import type { Snippet } from '$lib/types/svelte'

  const {
    content,
    danger,
    disabled,
    noclose,
    children
  }: {
    content: GlobalDialogContent
    danger?: boolean
    disabled?: boolean
    /** When set, the dialog has no "X" button and does not auto-close on cancel/click-away. */
    noclose?: boolean
    children?: Snippet
  } = $props()

  const globalDialog = useGlobalDialog()
  const cancel = () => {
    content.onCancel?.callback?.()
    if (!noclose) {
      globalDialog.dialog = null
    }
  }
  $effect(() => {
    globalDialog.onClickAway = cancel
  })
</script>

<div class="flex flex-col gap-4 p-4 sm:p-8" data-testid="box-generic-dialog">
  <div class="flex flex-nowrap justify-between">
    <div class="h5" data-testid="box-dialog-title">{content.title}</div>
    {#if !noclose}
      <button onclick={cancel} class="fd fd-x -m-4 btn-icon text-[24px]" aria-label="Close dialog"
      ></button>
    {/if}
  </div>
  <div
    class="-mr-4 scrollbar flex max-h-[calc(90vh-96px)] flex-col gap-4 overflow-auto pr-4 sm:-mr-8"
  >
    {#if content.description}
      <span class="whitespace-pre-wrap" data-testid="box-dialog-description">
        {content.description}
      </span>
    {/if}
    {#if content.scrollableContent}
      <div
        class="bg-surface-100-800 scrollbar max-h-[60vh] overflow-y-auto rounded border p-2 whitespace-pre-wrap"
        data-testid="box-dialog-scrollable-content"
      >
        {content.scrollableContent}
      </div>
    {/if}
    {@render children?.()}
  </div>
  {#if content.onSuccess}
    <div
      class="flex w-full flex-col-reverse gap-4 sm:flex-row sm:justify-end"
      data-testid="box-dialog-actions"
    >
      <button
        onclick={() => cancel()}
        class="btn preset-filled-surface-50-950 px-4"
        data-testid="btn-dialog-cancel"
      >
        {content.onCancel?.name ?? 'Cancel'}
      </button>
      <div>
        <button
          {disabled}
          onclick={content.onSuccess.callback}
          class="btn px-4 {danger
            ? 'preset-filled-error-500 font-semibold'
            : 'preset-filled-primary-500'}"
          data-testid={content.onSuccess['data-testid']}
        >
          {content.onSuccess.name}
        </button>
      </div>
      {#if disabled && content.onSuccess.disabledMessage}
        <Tooltip class="z-20 w-64" placement="top">{content.onSuccess.disabledMessage}</Tooltip>
      {/if}
    </div>
  {/if}
</div>
