<script lang="ts">
  import PipelineNameInput from '$lib/components/pipelines/PipelineNameInput.svelte'

  const {
    inputClass,
    btnClass,
    onSuccess
  }: { class?: string; inputClass?: string; btnClass?: string; onSuccess?: () => void } = $props()
</script>

<PipelineNameInput inputClass="input h-9 {inputClass}" {onSuccess}>
  {#snippet createButton(onclick)}
    <div class="flex justify-center">
      <button class="btn h-9 {btnClass}" {onclick}>
        <span class="fd fd fd-plus text-2xl"></span>
        New Pipeline
      </button>
    </div>
  {/snippet}

  {#snippet afterInput(error)}
    <div class="absolute top-10 z-20">
      {#if error}
        <div class="bg-white-dark bg-white-dark rounded-container px-2 py-2 text-error-500 shadow">
          {error}
        </div>
      {:else}
        <div
          class="bg-white-dark rounded-container px-2 py-2 whitespace-pre text-surface-600-400 shadow"
        >
          Press Enter to create
        </div>
      {/if}
    </div>
  {/snippet}
</PipelineNameInput>
