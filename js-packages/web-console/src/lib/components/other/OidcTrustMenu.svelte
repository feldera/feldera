<script lang="ts">
  import { asyncReadable } from '@square/svelte-store'
  import { tick } from 'svelte'
  import { page } from '$app/state'
  import GenericDialog from '$lib/components/dialogs/GenericDialog.svelte'
  import NewOidcTrustForm from '$lib/components/oidcTrust/NewOidcTrustForm.svelte'
  import { useGlobalDialog } from '$lib/compositions/layout/useGlobalDialog.svelte'
  import {
    deleteOidcTrust,
    getOidcTrustList,
    type OidcTrustDescr
  } from '$lib/services/pipelineManager'

  const tenantName = $derived(page.data.feldera?.tenantName ?? 'current tenant')
  const trusts = asyncReadable<OidcTrustDescr[]>([], getOidcTrustList, { reloadable: true })

  const globalDialog = useGlobalDialog()
  const thisDialog = globalDialog.dialog

  let showForm = $state(false)
  let trustForm: NewOidcTrustForm | undefined = $state()
  let scrollEl: HTMLDivElement | undefined = $state()

  // Reveal the form and scroll the dialog to it (the form sits below the list,
  // which can be long). Returns once the form is mounted.
  const revealForm = async () => {
    showForm = true
    await tick()
    scrollEl?.scrollTo({ top: scrollEl.scrollHeight, behavior: 'smooth' })
  }

  // Owner trusts live on the Admin page, not here, so this menu only ever shows
  // read/write/admin; fall back defensively if an owner trust ever appears.
  const duplicate = async (trust: OidcTrustDescr) => {
    await revealForm() // also mounts the form so its `fill` binding is set
    trustForm?.fill({
      name: `${trust.name}-copy`,
      issuer: trust.issuer,
      subject: trust.subject,
      audience: trust.audience ?? '',
      description: trust.description ?? '',
      role: trust.role === 'owner' ? 'read' : trust.role
    })
  }
</script>

<GenericDialog content={{ title: `Manage OIDC trusts — ${tenantName}` }}>
  <div bind:this={scrollEl} class="-mr-4 scrollbar h-full overflow-auto pr-4 sm:-mr-8 sm:pr-8">
    <p class="text-sm text-surface-800-200">
      Grant read/write/admin to workloads (CI, services) in tenant <b>{tenantName}</b> by trusting JWTs
      from an issuer. Platform-wide owner trusts are managed on the Admin page.
    </p>
    <div class="my-2 flex flex-col gap-2">
      {#each $trusts as trust}
        {#snippet deleteDialog()}
          <GenericDialog
            content={{
              title: `Delete trust ${trust.name}?`,
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
        <div class="flex flex-nowrap border-b border-surface-200-800 py-2">
          <div class="w-full">
            <div>
              {trust.name}
              <span class="text-sm text-surface-800-200">[{trust.role}]</span>
            </div>
            <div class="text-sm">
              <code>{trust.issuer}</code> · sub=<code>{trust.subject}</code>{#if trust.audience}
                · aud=<code>{trust.audience}</code>{/if}
            </div>
            {#if trust.description}
              <div class="text-sm text-surface-800-200">{trust.description}</div>
            {/if}
          </div>
          <button
            class="fd fd-copy-plus btn-icon text-[20px] hover:bg-surface-50-950"
            aria-label="Duplicate {trust.name} trust relationship"
            title="Copy these fields into the form below to create a similar trust"
            onclick={() => duplicate(trust)}
          ></button>
          <button
            class="fd fd-trash-2 btn-icon text-[20px] hover:bg-surface-50-950"
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
    {#if showForm}
      <NewOidcTrustForm
        bind:this={trustForm}
        onSuccess={() => {
          trusts.reload?.()
        }}
      ></NewOidcTrustForm>
    {:else}
      <div class="flex">
        <button class="btn preset-filled-primary-500" onclick={revealForm}>
          Create new trust
        </button>
      </div>
    {/if}
  </div>
</GenericDialog>
