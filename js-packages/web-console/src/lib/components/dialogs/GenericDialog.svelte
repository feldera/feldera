<script lang="ts">
  import { Tooltip } from 'common-ui'
  import {
    type GlobalDialogContent,
    useGlobalDialog
  } from '#lib/compositions/layout/useGlobalDialog.svelte.ts'
  import type { Snippet } from '#lib/types/svelte.ts'

  const {
    content,
    danger,
    disabled,
    noclose,
    swapActions,
    children
  }: {
    /** If neither content.onSuccess nor content.onError are provided the footer is not rendered.  */
    content: GlobalDialogContent
    danger?: boolean
    disabled?: boolean
    /** When set, the dialog has no "X" button and does not auto-close on cancel/click-away. */
    noclose?: boolean
    swapActions?: boolean
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

<div class="flex flex-col gap-3 p-4 sm:p-6" data-testid="box-generic-dialog">
  <div class="flex flex-nowrap items-start justify-between gap-4">
    <div class="text-lg font-semibold break-words" data-testid="box-dialog-title">
      {content.title}
    </div>
    {#if !noclose}
      <button
        onclick={cancel}
        class="fd fd-x -mt-0.5 -mr-1 btn-icon btn-icon-sm shrink-0 text-[16px] text-surface-700-300 hover:bg-row-hover"
        aria-label="Close dialog"
      ></button>
    {/if}
  </div>
  <div
    class="-mr-4 scrollbar flex max-h-[calc(90vh-116px)] flex-col gap-4 overflow-visible pr-4 sm:-mr-8 sm:pr-8"
  >
    {#if content.description}
      <span class="whitespace-pre-wrap text-surface-600-400" data-testid="box-dialog-description">
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
  {#if content.onSuccess || content.onCancel}
    <div class="flex w-full flex-nowrap justify-end gap-2 pt-3" data-testid="box-dialog-actions">
      <button
        onclick={() => cancel()}
        class="btn h-8 preset-outlined-surface-200-800 px-4"
        class:order-last={swapActions}
        data-testid="btn-dialog-cancel"
      >
        {content.onCancel?.name ?? 'Cancel'}
      </button>
      {#if content.onSuccess}
        <div class:order-first={swapActions}>
          <button
            {disabled}
            onclick={content.onSuccess.callback}
            class="btn h-8 px-4 {danger ? 'preset-filled-error-500' : 'preset-filled-primary-500'}"
            data-testid={content.onSuccess['data-testid']}
          >
            {content.onSuccess.name}
          </button>
        </div>
        {#if disabled && content.onSuccess.disabledMessage}
          <Tooltip class="z-20 w-64" placement="top">{content.onSuccess.disabledMessage}</Tooltip>
        {/if}
      {/if}
    </div>
  {/if}
</div>
