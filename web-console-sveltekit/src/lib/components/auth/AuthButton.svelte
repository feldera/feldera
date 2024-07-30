<script lang="ts">
  import { page } from '$app/stores'
  import { Tooltip } from '$lib/components/common/Tooltip.svelte'
  import { fade } from 'svelte/transition'
  import Popup from '../common/Popup.svelte'
  import AuthPopupMenu from './AuthPopupMenu.svelte'
</script>

{#if typeof $page.data.auth === 'object' && 'logout' in $page.data.auth}
  <Popup>
    {#snippet trigger(toggle)}
      <button
        onclick={toggle}
        class=" preset-filled-primary-500 ml-2 flex items-center gap-2 rounded font-semibold md:px-2">
        <span class="hidden pl-1 md:block">Logged in</span>

        <div class="bx bx-user-circle btn-icon text-[32px]"></div>
      </button>
    {/snippet}
    {#snippet content(close)}
      <div
        transition:fade={{ duration: 100 }}
        class="absolute right-0 z-10 max-h-[400px] w-[calc(100vw-100px)] max-w-[400px] justify-end overflow-y-auto rounded bg-white shadow-md dark:bg-black">
        <AuthPopupMenu user={$page.data.auth.userInfo} signOut={$page.data.auth.logout}
        ></AuthPopupMenu>
      </div>
    {/snippet}
  </Popup>
{:else}
  <button class="bx bx-lock-open-alt btn-icon text-surface-600-400 cursor-default text-[24px]"
  ></button>
  <Tooltip class="text-surface-950-50 bg-white dark:bg-black" placement="left">
    Authentication is disabled
  </Tooltip>
{/if}
