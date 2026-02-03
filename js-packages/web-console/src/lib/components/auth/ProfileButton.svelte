<script lang="ts">
  import { fade } from 'svelte/transition'
  import { goto } from '$app/navigation'
  import { page } from '$app/state'
  import CurrentTenant from '$lib/components/auth/CurrentTenant.svelte'
  import Popup from '$lib/components/common/Popup.svelte'
  import DarkModeSwitch from '$lib/components/layout/userPopup/DarkModeSwitch.svelte'
  import ApiKeyMenu from '$lib/components/other/ApiKeyMenu.svelte'
  import VersionDisplay from '$lib/components/version/VersionDisplay.svelte'
  import type { ClusterHealthStatus } from '$lib/compositions/health/useClusterHealth.svelte'
  import { useGlobalDialog } from '$lib/compositions/layout/useGlobalDialog.svelte'
  import type { HealthEventType } from '$lib/functions/pipelines/health'
  import type { AuthDetails } from '$lib/types/auth'
  import type { Snippet } from '$lib/types/svelte'

  const {
    compactBreakpoint = '',
    healthStatus
  }: { compactBreakpoint?: string; healthStatus: ClusterHealthStatus } = $props()
  const auth = page.data.auth as AuthDetails | undefined

  const globalDialog = useGlobalDialog()

  let combinedStatus: HealthEventType = $derived.by(() => {
    const all = Object.values(healthStatus)
    return all.some((e) => e === 'major_issue')
      ? 'major_issue'
      : all.some((e) => e === 'unhealthy')
        ? 'unhealthy'
        : 'healthy'
  })
</script>

{#snippet profileItemButton(label: string, icon: Snippet, onclick: () => void)}
  <button class="group flex flex-nowrap items-center justify-start gap-2 font-medium" {onclick}>
    {@render icon()}
    <span class="mr-auto">{label}</span>
    <span class="fd fd-chevron-right rounded p-2 text-[20px] group-hover:bg-surface-50-950"></span>
  </button>
{/snippet}

<Popup>
  {#snippet trigger(toggle)}
    {#if typeof auth === 'object' && 'logout' in auth}
      <button onclick={toggle} class="flex items-center gap-2 rounded font-semibold">
        <div class="hidden {compactBreakpoint}block w-2"></div>
        <span class="hidden {compactBreakpoint}block">Logged in</span>
        <div class="hidden {compactBreakpoint}block w-1"></div>

        <div class="fd fd-circle-user btn-icon preset-tonal-surface text-[20px]">
          <div class="hidden {compactBreakpoint}block w-2"></div>
        </div>
      </button>
    {:else}
      <button
        onclick={toggle}
        class="fd fd-lock-open btn-icon preset-tonal-surface text-[20px]"
        aria-label="Open settings popup"
      >
      </button>
    {/if}
  {/snippet}
  {#snippet content(close)}
    <div
      transition:fade={{ duration: 100 }}
      class="bg-white-dark absolute right-0 z-30 scrollbar w-[calc(100vw-100px)] max-w-[400px] justify-end rounded-container shadow-md"
    >
      <div class="flex flex-col gap-3 p-4">
        {#snippet apiKeysIcon()}
          <div class="fd fd-key text-[20px]"></div>
        {/snippet}

        {#if typeof auth === 'object' && 'logout' in auth}
          {@render profileItemButton(
            'Manage API keys',
            apiKeysIcon,
            () => (globalDialog.dialog = apiKeyDialog)
          )}

          <div class="hr"></div>
        {/if}
        <DarkModeSwitch class="pl-7"></DarkModeSwitch>
        <div class="hr"></div>
        {#if typeof auth === 'object' && 'logout' in auth}
          <div class="flex gap-1">
            {#if auth.profile.picture}
              <img class="h-6 w-6 rounded-full" src={auth.profile.picture} alt="User avatar" />
            {:else}
              <div class="fd fd-circle-user h-6 w-6 rounded-full text-[24px]"></div>
            {/if}
            <div class="">
              <div class="h4 font-normal break-all" class:italic={!auth.profile.name}>
                {auth.profile.name || 'anonymous'}
              </div>
              <div class="">{auth.profile.email}</div>
            </div>
          </div>
        {:else}
          <div class="text-surface-700-300">Authentication is disabled</div>
        {/if}
        <CurrentTenant class="pl-7"></CurrentTenant>
        {#if typeof auth === 'object' && 'logout' in auth}
          <div class="hr"></div>
          <button
            class="-ml-4 btn justify-start"
            onclick={async () => {
              // Redirect to home page, otherwise the auth client inserts the current page
              // which is not whitelisted by the auth provider
              await auth.logout({ callbackUrl: '/' })
            }}
          >
            <div class=" fd fd-log-out text-[20px]"></div>
            Sign Out
          </button>
        {/if}
        <div class="hr"></div>
        {#snippet healthIcon()}
          <div
            class="m-2 h-2.5 w-2.5 rounded-full text-[64px] {combinedStatus === 'healthy'
              ? 'bg-success-500'
              : combinedStatus === 'unhealthy'
                ? 'bg-warning-500'
                : 'bg-error-500'}"
          ></div>
        {/snippet}
        {@render profileItemButton('Feldera Health', healthIcon, () => goto('/health'))}
        <div class="hr"></div>
        <VersionDisplay></VersionDisplay>
      </div>
    </div>
  {/snippet}
</Popup>

{#snippet apiKeyDialog()}
  <ApiKeyMenu></ApiKeyMenu>
{/snippet}
