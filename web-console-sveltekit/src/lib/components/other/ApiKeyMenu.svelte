<script lang="ts">
  import { deleteApiKey, getApiKeys } from '$lib/services/pipelineManager'
  import { asyncReadable } from '@square/svelte-store'
  import CreateApiKeyMenu from './CreateApiKeyForm.svelte'
  import { useGlobalDialog } from '$lib/compositions/useGlobalDialog.svelte'
  import DeleteDialog, { deleteDialogProps } from '$lib/components/dialogs/DeleteDialog.svelte'

  const apiKeys = asyncReadable([], getApiKeys)
  const globalDialog = useGlobalDialog()
  let thisDialog = globalDialog.dialog
</script>

<div class="flex flex-col">
  <div class="sticky top-0 flex flex-col gap-4 p-4 bg-surface-50-950">
    <div class="h5 font-normal">API keys</div>

    <button
      class="btn w-full preset-outlined-primary-500"
      onclick={() => (globalDialog.dialog = createAiKeyDialog)}
    >
      Generate new key
    </button>
  </div>
  <div class="flex flex-col gap-2 p-4 pt-0">
    {#each $apiKeys as key}
      {#snippet deleteDialog()}
        <DeleteDialog
          {...deleteDialogProps('Delete', (name) => `${name} API key`, deleteApiKey)(key.name)}
          onClose={() => (globalDialog.dialog = thisDialog)}
        ></DeleteDialog>
      {/snippet}
      <div class="flex flex-nowrap">
        <div class=" w-full">
          <div>
            {key.name}
            [{key.scopes}]
          </div>

          <div class="text-sm">{key.id}</div>
        </div>
        <button
          class="bx bx-trash-alt btn-icon text-[24px]"
          onclick={() => (globalDialog.dialog = deleteDialog)}
        ></button>
      </div>
    {:else}
      No generated API keys
    {/each}
  </div>
</div>

{#snippet createAiKeyDialog()}
  <div class="h-96">
    <CreateApiKeyMenu onClose={() => (globalDialog.dialog = thisDialog)}></CreateApiKeyMenu>
  </div>
{/snippet}
