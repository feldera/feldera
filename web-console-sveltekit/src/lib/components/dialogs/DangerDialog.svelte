<script lang="ts">
  import type { GlobalDialogContent } from '$lib/compositions/useGlobalDialog.svelte'

  let { content, onClose }: { content: GlobalDialogContent; onClose?: () => void } = $props()
</script>

<div class="px-4 pb-4 pt-5 sm:p-6 sm:pb-4">
  <div class="sm:flex sm:items-start">
    <div
      class="bg-error-50-950 mx-auto flex h-12 w-12 flex-shrink-0 items-center justify-center rounded-full sm:mx-0 sm:h-10 sm:w-10">
      <div class="bx bx-error text-error-500 text-[24px]"></div>
    </div>
    <div class="mt-3 text-center sm:ml-4 sm:mt-0 sm:text-left">
      <h3 class="text-surface-900-100 text-base font-semibold leading-6" id="modal-title">
        {content.title}
      </h3>
      <div class="mt-2">
        <p class="text-surface-500 text-sm">
          {content.description}
        </p>
      </div>
    </div>
  </div>
</div>
<div class="bg-surface-100-900 px-4 py-3 sm:flex sm:flex-row-reverse sm:px-6">
  <button
    type="button"
    onclick={() => {
      content!.onSuccess.callback()
      onClose?.()
    }}
    data-testid={content.onSuccess['data-testid']}
    class="inline-flex w-full justify-center rounded-md bg-red-600 px-3 py-2 text-sm font-semibold text-white shadow-sm hover:bg-red-500 sm:ml-3 sm:w-auto">
    {content.onSuccess.name}
  </button>
  <button
    type="button"
    onclick={onClose}
    class="text-surface-900-100 bg-surface-200-800 hover:bg-surface-300-700 mt-3 inline-flex w-full justify-center rounded-md px-3 py-2 text-sm font-semibold shadow-sm sm:mt-0 sm:w-auto">
    Cancel
  </button>
</div>
