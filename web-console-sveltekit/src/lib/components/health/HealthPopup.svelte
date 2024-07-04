<script lang="ts">
  import { useSystemErrors } from '$lib/compositions/health/systemErrors'
  import Tooltip from 'sv-tooltip'
  import Popup from '$lib/components/common/Popup.svelte'
  import { fade } from 'svelte/transition'
  import HealthMenu from './HealthMenu.svelte'
  const systemErrors = useSystemErrors()
</script>

{#if $systemErrors.length}
  <Popup>
    {#snippet trigger(open)}
      <button
        onclick={open}
        class="bx bx-error btn-icon cursor-pointer text-[24px] !text-warning-500 preset-tonal-surface"
      >
      </button>
      <span class="badge-icon absolute -right-2 -top-2 text-sm preset-filled-error-500">
        {$systemErrors.length}
      </span>
    {/snippet}
    {#snippet content()}
      <div
        transition:fade={{ duration: 100 }}
        class="absolute right-0 z-10 max-h-[500px] w-[calc(100vw-100px)] max-w-[600px] justify-end overflow-y-auto rounded bg-white shadow-md dark:bg-black"
      >
        <HealthMenu {systemErrors}></HealthMenu>
      </div>
    {/snippet}
  </Popup>
{:else}
  <Tooltip tip="No errors detected in Feldera deployment" active={true}>
    <button class="bx bx-check-circle btn-icon text-[24px] !text-success-500 preset-tonal-surface">
    </button>
  </Tooltip>
{/if}
