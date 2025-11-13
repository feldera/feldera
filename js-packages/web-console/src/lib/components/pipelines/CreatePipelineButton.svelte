<script lang="ts">
  import PipelineNameInput from '$lib/components/pipelines/PipelineNameInput.svelte'
  let {
    inputClass,
    btnClass,
    onSuccess
  }: { class?: string; inputClass?: string; btnClass?: string; onSuccess?: () => void } = $props()
</script>

<PipelineNameInput inputClass="input h-9 {inputClass}" {onSuccess}>
  {#snippet createButton(onclick)}
    <div class="flex justify-center {btnClass}">
      <button class="btn" {onclick}>
        <span class="fd fd fd-plus text-2xl"></span>
        New Pipeline
      </button>
    </div>
  {/snippet}

  {#snippet afterInput(error)}
    <div class="absolute top-10 z-10">
      {#if error}
        <div class="bg-white-dark bg-white-dark rounded-container px-2 py-2 text-error-500 shadow">
          {error}
        </div>
      {:else}
        <div
          class="bg-white-dark whitespace-pre rounded-container px-2 py-2 shadow text-surface-600-400"
        >
          Press Enter to create
        </div>
      {/if}
    </div>
  {/snippet}
</PipelineNameInput>
