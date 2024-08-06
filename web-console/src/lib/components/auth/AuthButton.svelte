<script lang="ts">
  import { page } from '$app/stores'
  import { Tooltip } from '$lib/components/common/Tooltip.svelte'
  import { fade } from 'svelte/transition'
  import Popup from '../common/Popup.svelte'
  import AuthPopupMenu from './AuthPopupMenu.svelte'
  import type { AuthDetails } from '$lib/types/auth'

  const { compactBreakpoint = '' }: { compactBreakpoint?: string } = $props()
  const auth = $page.data.auth as AuthDetails | undefined
</script>

{#if typeof auth === 'object' && 'logout' in auth}
  <Popup>
    {#snippet trigger(toggle)}
      <button
        onclick={toggle}
        class=" ml-2 flex items-center gap-2 rounded font-semibold preset-filled-primary-500"
      >
        <div class="hidden {compactBreakpoint}block w-2"></div>
        <span class="hidden {compactBreakpoint}block">Logged in</span>
        <div class="hidden {compactBreakpoint}block w-1"></div>

        <div class="bx bx-user-circle btn-icon text-[32px]">
          <div class="hidden {compactBreakpoint}block w-2"></div>
        </div>
      </button>
    {/snippet}
    {#snippet content(close)}
      <div
        transition:fade={{ duration: 100 }}
        class="absolute right-0 z-10 max-h-[400px] w-[calc(100vw-100px)] max-w-[400px] justify-end overflow-y-auto rounded bg-white shadow-md dark:bg-black"
      >
        <AuthPopupMenu user={auth.profile} signOut={auth.logout}></AuthPopupMenu>
      </div>
    {/snippet}
  </Popup>
{:else}
  <button class="bx bx-lock-open-alt btn-icon cursor-default text-[24px] text-surface-600-400"
  ></button>
  <Tooltip class="bg-white text-surface-950-50 dark:bg-black" placement="left">
    Authentication is disabled
  </Tooltip>
{/if}
