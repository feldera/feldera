<script lang="ts">
  import PipelineNameInput from '$lib/components/pipelines/PipelineNameInput.svelte'
  let { class: _class }: { class?: string } = $props()
</script>

<PipelineNameInput inputClass="input h-9 {_class}">
  {#snippet createButton(onclick)}
    <div class="flex justify-center">
      <button class="btn preset-filled-primary-500" {onclick}>
        <span class="fd fd-add text-2xl"></span>
        New Pipeline
      </button>
    </div>
  {/snippet}

  {#snippet afterInput(error)}
    <div class="absolute top-10 z-10">
      {#if error}
        <div class="bg-white-black mt-2 px-1 text-error-500">{error}</div>
      {:else}
        <div class="whitespace-pre pt-2 text-surface-600-400">Press Enter to create</div>
      {/if}
    </div>
  {/snippet}
</PipelineNameInput>
