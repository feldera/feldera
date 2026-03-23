<script lang="ts">
  import { asyncReadable } from '@square/svelte-store'
  import NewApiKeyForm from '$lib/components/apiKey/NewApiKeyForm.svelte'
  import GenericDialog from '$lib/components/dialogs/GenericDialog.svelte'
  import { useGlobalDialog } from '$lib/compositions/layout/useGlobalDialog.svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'

  const api = usePipelineManager()
  const apiKeys = asyncReadable([], api.getApiKeys, { reloadable: true })

  const globalDialog = useGlobalDialog()
  const thisDialog = globalDialog.dialog
</script>

<GenericDialog content={{ title: 'Manage API keys' }}>
  <div class="scrollbar flex max-h-[80vh] flex-col gap-2 overflow-auto">
    {#each $apiKeys as key}
      {#snippet deleteDialog()}
        <GenericDialog
          content={{
            title: `Delete ${key.name} API key?`,
            description: 'Are you sure? This action is irreversible.',
            onSuccess: {
              name: 'Delete',
              callback: async () => {
                await api.deleteApiKey(key.name)
                globalDialog.dialog = thisDialog
              },
              'data-testid': 'button-confirm-delete'
            },
            onCancel: {
              callback: () => {
                globalDialog.dialog = thisDialog
              }
            }
          }}
          noclose
          danger
        ></GenericDialog>
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
          onclick={() => {
            globalDialog.dialog = deleteDialog
          }}
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
</GenericDialog>
