<script lang="ts">
  import { asyncReadable } from '@square/svelte-store'
  import NewOidcTrustForm from '$lib/components/oidcTrust/NewOidcTrustForm.svelte'
  import GenericDialog from '$lib/components/dialogs/GenericDialog.svelte'
  import { useGlobalDialog } from '$lib/compositions/layout/useGlobalDialog.svelte'
  import {
    deleteOidcTrust,
    getOidcTrustList,
    type OidcTrustDescr
  } from '$lib/services/pipelineManager'

  const trusts = asyncReadable<OidcTrustDescr[]>([], getOidcTrustList, { reloadable: true })

  const globalDialog = useGlobalDialog()
  const thisDialog = globalDialog.dialog
</script>

<GenericDialog content={{ title: 'Manage OIDC trust relationships' }}>
  <div class="scrollbar flex max-h-[80vh] flex-col gap-2 overflow-auto">
    {#each $trusts as trust}
      {#snippet deleteDialog()}
        <GenericDialog
          content={{
            title: `Delete ${trust.name}?`,
            description: 'Are you sure? This action is irreversible.',
            onSuccess: {
              name: 'Delete',
              callback: async () => {
                await deleteOidcTrust(trust.name)
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
        <div class="w-full">
          <div>
            {trust.name}
            <span class="text-xs opacity-70">[{trust.scopes.join(', ')}]</span>
          </div>
          <div class="text-sm">
            <code>{trust.issuer}</code> · sub=<code>{trust.subject}</code
            >{#if trust.audience}
              · aud=<code>{trust.audience}</code>{/if}
          </div>
          {#if trust.description}
            <div class="text-xs opacity-70">{trust.description}</div>
          {/if}
        </div>
        <button
          class="fd fd-trash-2 btn-icon text-[20px]"
          aria-label="Delete {trust.name} trust relationship"
          onclick={() => {
            globalDialog.dialog = deleteDialog
          }}
        ></button>
      </div>
    {:else}
      No OIDC trust relationships configured
    {/each}
  </div>
  <NewOidcTrustForm
    onSuccess={() => {
      trusts.reload?.()
    }}
  ></NewOidcTrustForm>
</GenericDialog>
