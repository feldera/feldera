<script lang="ts">
  import { fade } from 'svelte/transition'
  import { page } from '$app/state'
  import CurrentTenant from '$lib/components/auth/CurrentTenant.svelte'
  import ProfilePopupMenu from '$lib/components/auth/ProfilePopupMenu.svelte'
  import Popup from '$lib/components/common/Popup.svelte'
  import DarkModeSwitch from '$lib/components/layout/userPopup/DarkModeSwitch.svelte'
  import VersionDisplay from '$lib/components/version/VersionDisplay.svelte'
  import type { AuthDetails } from '$lib/types/auth'

  const { compactBreakpoint = '' }: { compactBreakpoint?: string } = $props()
  const auth = page.data.auth as AuthDetails | undefined
</script>

{#if typeof auth === 'object' && 'logout' in auth}
  <Popup>
    {#snippet trigger(toggle)}
      <button onclick={toggle} class="flex items-center gap-2 rounded font-semibold">
        <div class="hidden {compactBreakpoint}block w-2"></div>
        <span class="hidden {compactBreakpoint}block">Logged in</span>
        <div class="hidden {compactBreakpoint}block w-1"></div>

        <div class="fd fd-circle-user btn-icon preset-tonal-surface text-[20px]">
          <div class="hidden {compactBreakpoint}block w-2"></div>
        </div>
      </button>
    {/snippet}
    {#snippet content(close)}
      <div
        transition:fade={{ duration: 100 }}
        class="bg-white-dark absolute right-0 z-30 scrollbar w-[calc(100vw-100px)] max-w-[400px] justify-end rounded-container shadow-md"
      >
        <ProfilePopupMenu {...auth}></ProfilePopupMenu>
      </div>
    {/snippet}
  </Popup>
{:else}
  <!-- <button class="fd fd-lock-open btn-icon cursor-default text-[20px] text-surface-600-400"></button>
  <Tooltip class="bg-white text-surface-950-50 dark:bg-black" placement="left">
    Authentication is disabled
  </Tooltip> -->

  <Popup>
    {#snippet trigger(toggle)}
      <button
        onclick={toggle}
        class="fd fd-lock-open btn-icon preset-tonal-surface text-[20px]"
        aria-label="Open settings popup"
      >
      </button>
    {/snippet}
    {#snippet content(close)}
      <div
        transition:fade={{ duration: 100 }}
        class="bg-white-dark absolute right-0 z-30 scrollbar flex max-h-[400px] w-[calc(100vw-16px)] max-w-[360px] flex-col justify-end gap-4 rounded-container p-4 shadow-md sm:max-w-[400px]"
      >
        <div class="text-surface-700-300">Authentication is disabled</div>

        <CurrentTenant></CurrentTenant>
        <div class="hr"></div>
        <DarkModeSwitch></DarkModeSwitch>
        <VersionDisplay></VersionDisplay>
      </div>
    {/snippet}
  </Popup>
{/if}
