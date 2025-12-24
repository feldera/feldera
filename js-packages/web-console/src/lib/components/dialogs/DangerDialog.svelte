<script lang="ts">
  import type { GlobalDialogContent } from '$lib/compositions/layout/useGlobalDialog.svelte'

  const { content, onClose }: { content: GlobalDialogContent; onClose?: () => void } = $props()
</script>

<div class="p-4 sm:p-8">
  <div class="flex flex-col gap-4">
    <div class="flex flex-nowrap justify-between">
      <div class="h5">{content.title}</div>
      <button
        class="fd fd-x -m-4 btn-icon text-[24px]"
        onclick={onClose}
        aria-label="Confirm dangerous action"
      ></button>
    </div>
    <span class="whitespace-pre-wrap">
      {content.description}
    </span>
    {#if content.scrollableContent}
      <div
        class="bg-surface-100-800 scrollbar max-h-[60vh] overflow-y-auto rounded border p-2 whitespace-pre-wrap"
      >
        {content.scrollableContent}
      </div>
    {/if}
  </div>
  <div class="flex flex-col-reverse gap-4 pt-4 sm:flex-row sm:justify-end">
    <button class="btn preset-filled-surface-50-950 px-4" onclick={onClose}> Cancel </button>
    <button
      class="btn preset-filled-error-500 px-4 font-semibold"
      onclick={async () => {
        await content!.onSuccess.callback()
        onClose?.()
      }}
      data-testid={content.onSuccess['data-testid']}
    >
      {content.onSuccess.name}
    </button>
  </div>
</div>
