<script lang="ts">
  import { fade } from 'svelte/transition'
  import { page } from '$app/state'
  import CurrentTenant from '$lib/components/auth/CurrentTenant.svelte'
  import RBAC from '$lib/components/auth/RBAC.svelte'
  import Popup from '$lib/components/common/Popup.svelte'
  import DarkModeSwitch from '$lib/components/layout/userPopup/DarkModeSwitch.svelte'
  import ApiKeyMenu from '$lib/components/other/ApiKeyMenu.svelte'
  import OidcTrustMenu from '$lib/components/other/OidcTrustMenu.svelte'
  import VersionDisplay from '$lib/components/version/VersionDisplay.svelte'
  import type { ClusterHealthStatus } from '$lib/compositions/health/useClusterHealth.svelte'
  import { useGlobalDialog } from '$lib/compositions/layout/useGlobalDialog.svelte'
  import { type ClusterEventType, worstClusterStatus } from '$lib/functions/pipelines/health'
  import { resolve } from '$lib/functions/svelte'
  import type { AuthDetails } from '$lib/types/auth'
  import type { Snippet } from '$lib/types/svelte'

  const {
    compactBreakpoint = '',
    healthStatus
  }: { compactBreakpoint?: string; healthStatus: ClusterHealthStatus | undefined } = $props()
  const auth = page.data.auth as AuthDetails | undefined

  const globalDialog = useGlobalDialog()

  // Undefined while no poll has answered; the dot then shows no verdict.
  // Stale data cannot claim health, but a recorded issue keeps its own colour.
  let combinedStatus: ClusterEventType | 'stale' | undefined = $derived.by(() => {
    if (!healthStatus) {
      return undefined
    }
    const worst = worstClusterStatus([healthStatus.api, healthStatus.compiler, healthStatus.runner])
    return healthStatus.stale && worst === 'healthy' ? 'stale' : worst
  })
</script>

{#snippet profileItemButton(
  label: string,
  icon: Snippet,
  action: { onclick: () => void } | { href?: string }
)}
  <svelte:element
    this={'onclick' in action ? 'button' : 'a'}
    class="-mx-2 flex flex-nowrap items-center justify-start gap-1.5 rounded px-2 font-medium hover:bg-surface-50-950"
    {...action}
  >
    <div class="flex w-7 shrink-0 justify-center">{@render icon()}</div>
    <span class="mr-auto">{label}</span>
    <span class="fd fd-chevron-right p-2.5 text-[16px]"></span>
  </svelte:element>
{/snippet}

<Popup>
  {#snippet trigger(toggle)}
    {#if typeof auth === 'object' && 'logout' in auth}
      <button onclick={toggle} class="flex items-center gap-2 rounded font-semibold">
        <div class="hidden {compactBreakpoint}block w-2"></div>
        <span class="hidden {compactBreakpoint}block">Logged in</span>
        <div class="hidden {compactBreakpoint}block w-1"></div>

        <div class="fd fd-circle-user btn-icon preset-tonal-surface text-[16px]">
          <div class="hidden {compactBreakpoint}block w-2"></div>
        </div>
      </button>
    {:else}
      <button
        onclick={toggle}
        class="fd fd-lock-open btn-icon preset-tonal-surface text-[16px]"
        aria-label="Open settings popup"
      >
      </button>
    {/if}
  {/snippet}
  {#snippet content(close)}
    <div
      transition:fade={{ duration: 100 }}
      class="bg-white-dark absolute right-0 z-30 scrollbar w-[calc(100vw-100px)] max-w-[400px] justify-end rounded-container shadow-md dark:border dark:border-surface-950-50/10"
    >
      <div class="flex flex-col gap-3 p-4">
        {#snippet apiKeysIcon()}
          <div class="fd fd-key text-[16px]"></div>
        {/snippet}
        {#snippet oidcTrustIcon()}
          <div class="fd fd-user text-[16px]"></div>
        {/snippet}
        {#snippet adminIcon()}
          <div class="fd fd-shield text-[16px]"></div>
        {/snippet}
        {#snippet healthIcon()}
          <div
            class="h-2.5 w-2.5 rounded-full {combinedStatus === undefined
              ? 'border-2 border-surface-400'
              : combinedStatus === 'healthy'
                ? 'bg-success-500'
                : combinedStatus === 'transitioning'
                  ? 'bg-blue-500'
                  : combinedStatus === 'unhealthy' || combinedStatus === 'stale'
                    ? 'bg-warning-500'
                    : 'bg-error-500'}"
          ></div>
        {/snippet}

        {#if typeof auth === 'object' && 'logout' in auth}
          <div class="flex flex-col gap-0.5 rounded-lg bg-surface-950-50/[0.06] px-3.5 py-2">
            <div class="font-medium break-all" class:italic={!auth.profile.name}>
              {auth.profile.name || 'anonymous'}
            </div>
            <div class="flex flex-col gap-0.5 text-sm font-medium text-surface-600-400">
              <div class="break-all">{auth.profile.email}</div>
              <CurrentTenant></CurrentTenant>
            </div>
          </div>
          <div class="-my-1 flex flex-col gap-0.5">
            <RBAC require="write:tenant_member">
              {@render profileItemButton('Admin Dashboard', adminIcon, { href: resolve('/admin') })}
            </RBAC>
            <RBAC require="write:api_key">
              {@render profileItemButton('Manage API keys', apiKeysIcon, {
                onclick: () => (globalDialog.dialog = apiKeyDialog)
              })}
            </RBAC>
            <RBAC require="write:oidc_trust">
              {@render profileItemButton('Manage OIDC trust', oidcTrustIcon, {
                onclick: () => (globalDialog.dialog = oidcTrustDialog)
              })}
            </RBAC>
          </div>
        {:else}
          <div class="text-surface-700-300">Authentication is disabled</div>
          <CurrentTenant class="pl-8.5"></CurrentTenant>
        {/if}
        <div class="hr border-surface-100-900"></div>
        <DarkModeSwitch></DarkModeSwitch>
        <div class="hr border-surface-100-900"></div>
        <div class="-mt-1 flex flex-col gap-1">
          {#if typeof auth === 'object' && 'logout' in auth}
            <button
              class="-mx-2 flex h-9 items-center gap-1.5 rounded px-2 font-medium hover:bg-surface-50-950"
              onclick={async () => {
                // Redirect to home page, otherwise the auth client inserts the current page
                // which is not whitelisted by the auth provider
                await auth.logout({ callbackUrl: '/' })
              }}
            >
              <div class="flex w-7 shrink-0 justify-center">
                <div class="fd fd-log-out text-[16px]"></div>
              </div>
              Sign Out
            </button>
          {/if}
          <RBAC require="read:cluster_health">
            {@render profileItemButton('Feldera Health', healthIcon, { href: resolve('/health') })}
          </RBAC>
          <VersionDisplay></VersionDisplay>
        </div>
      </div>
    </div>
  {/snippet}
</Popup>

{#snippet apiKeyDialog()}
  <ApiKeyMenu></ApiKeyMenu>
{/snippet}

{#snippet oidcTrustDialog()}
  <OidcTrustMenu></OidcTrustMenu>
{/snippet}
