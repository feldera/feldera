<script lang="ts">
  import { useSystemErrors } from '$lib/compositions/health/systemErrors'
  import { Tooltip } from 'flowbite-svelte'
  import Popup from '$lib/components/common/Popup.svelte'
  import { fade } from 'svelte/transition'
  import HealthMenu from './HealthMenu.svelte'
  const systemErrors = useSystemErrors()
</script>

{#if $systemErrors.length}
  <Popup>
    {#snippet trigger(toggle)}
      <button
        onclick={toggle}
        class="bx bx-error btn-icon cursor-pointer text-[24px] !text-warning-500 preset-tonal-surface"
      >
      </button>
      <span class="badge-icon absolute -right-2 -top-2 text-sm preset-filled-error-500">
        {$systemErrors.length}
      </span>
    {/snippet}
    {#snippet content(close)}
      <div
        transition:fade={{ duration: 100 }}
        class="absolute right-0 z-10 max-h-[500px] w-[calc(100vw-100px)] max-w-[600px] justify-end overflow-y-auto rounded bg-white shadow-md dark:bg-black"
      >
        <HealthMenu {systemErrors} {close}></HealthMenu>
      </div>
    {/snippet}
  </Popup>
{:else}
  <button class="bx bx-check-circle btn-icon text-[24px] !text-success-500 preset-tonal-surface">
  </button>
  <Tooltip class="!bg-surface-950-50 text-surface-50-950" placement="left">
    No errors detected in Feldera deployment
  </Tooltip>
{/if}
