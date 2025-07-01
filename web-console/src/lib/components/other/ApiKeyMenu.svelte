<script lang="ts">
  import { asyncReadable } from '@square/svelte-store'
  import { useGlobalDialog } from '$lib/compositions/useGlobalDialog.svelte'
  import DeleteDialog, { deleteDialogProps } from '$lib/components/dialogs/DeleteDialog.svelte'
  import NewApiKeyForm from '$lib/components/apiKey/NewApiKeyForm.svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'

  const api = usePipelineManager()
  const apiKeys = asyncReadable([], api.getApiKeys, { reloadable: true })

  const globalDialog = useGlobalDialog()
  let thisDialog = globalDialog.dialog
</script>

<div class="flex h-fit max-h-[90vh] flex-col sm:p-4">
  <div class="bg-white-dark sticky top-0 flex flex-col gap-4 p-4">
    <div class="flex flex-nowrap justify-between">
      <div class="h5 font-medium">Manage API keys</div>
      <button
        class="fd fd-x btn btn-icon text-[20px] -m-4"
        aria-label="Close"
        onclick={() => (globalDialog.dialog = null)}
      ></button>
    </div>
  </div>
  <div class="flex flex-col gap-2 p-4 pt-0">
    {#each $apiKeys as key}
      {#snippet deleteDialog()}
        <DeleteDialog
          {...deleteDialogProps('Delete', (name) => `${name} API key`, api.deleteApiKey)(key.name)}
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
          class="fd fd-trash-2 btn-icon text-[20px]"
          aria-label="Delete {key.name} API key"
          onclick={() => (globalDialog.dialog = deleteDialog)}
        ></button>
      </div>
    {:else}
      No generated API keys
    {/each}
  </div>
  <NewApiKeyForm
    onSuccess={() => {
      apiKeys.reload?.()
    }}
  ></NewApiKeyForm>
  <!-- <button
    class="btn w-full preset-outlined-primary-500"
    onclick={() => (globalDialog.dialog = createAiKeyDialog)}
  >
    Generate new key
  </button> -->
</div>
