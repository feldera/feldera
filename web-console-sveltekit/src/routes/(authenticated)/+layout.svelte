<script lang="ts">
  import HealthPopup from '$lib/components/health/HealthPopup.svelte'
  import Drawer from '$lib/components/layout/Drawer.svelte'
  import GlobalModal from '$lib/components/dialogs/GlobalModal.svelte'
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'
  import FelderaLogoColor from '$assets/images/feldera/LogoSolid.svg?component'
  import FelderaLogoWhite from '$assets/images/feldera/LogoWhite.svg?component'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  import PipelinesList from '$lib/components/pipelines/List.svelte'
  import type { Snippet } from 'svelte'
  import { asyncReadable, derived, readable } from '@square/svelte-store'
  import { getPipelines, type PipelineThumb } from '$lib/services/pipelineManager'
  import { onMount } from 'svelte'
  import { Store } from 'runed'
  import { navItems } from '$lib/functions/navigation/items'
  import { useGlobalDialog } from '$lib/compositions/useGlobalDialog.svelte'
  import AuthButton from '$lib/components/auth/AuthButton.svelte'
  console.log('render layout 1-1')
  const dialog = useGlobalDialog()

  let { children } = $props<{ children: Snippet }>()
  let { darkMode, toggleDarkMode } = useDarkMode()
  let showDrawer = useLocalStorage('layout/drawer', false)

  let pipelinesStore = asyncReadable([], getPipelines, { reloadable: true })
  onMount(() => {
    let interval = setInterval(() => pipelinesStore.reload?.(), 2000)
    return () => {
      clearInterval(interval)
    }
  })
  let pipelines = new Store(pipelinesStore)
  console.log('render layout 1-2')
</script>

<div class="flex h-full">
  <Drawer width="w-72" bind:open={showDrawer.value} side="left">
    <div class="flex w-full flex-col gap-1">
      <a href="/">
        {#if darkMode.value === 'dark'}
          <FelderaLogoWhite class="w-40 p-3"></FelderaLogoWhite>
        {:else}
          <FelderaLogoColor class="w-40 p-3"></FelderaLogoColor>
        {/if}
      </a>
      <PipelinesList pipelines={pipelines.current}></PipelinesList>
    </div>
  </Drawer>
  <div class="flex h-full w-full flex-col">
    <div class="flex justify-between p-3">
      <div class="flex">
        <button
          class="btn-icon"
          onclick={() => {
            showDrawer.value = !showDrawer.value
          }}>
          <i class="bx bx-menu text-[24px]"></i>
        </button>
      </div>
      <div class="flex"></div>
      <div class="flex gap-2">
        {#each navItems({ showSettings: false }) as item}
          <a
            href={Array.isArray(item.path) ? item.path[0] : item.path}
            class="preset-grayout-surface flex flex-nowrap items-center justify-center">
            <div class="flex w-9 justify-center">
              <div class={item.class + ' text-[24px]'}></div>
            </div>
            <span class="hidden md:inline">{item.title}</span>
          </a>
        {/each}
        <!-- <HealthPopup></HealthPopup> -->
        <!-- <button
          onclick={toggleDarkMode}
          class={'btn-icon preset-tonal-surface text-[24px] ' +
            (darkMode.value === 'dark' ? 'bx bx-sun ' : 'bx bx-moon ')}></button> -->
        <AuthButton></AuthButton>
      </div>
    </div>
    {@render children()}
  </div>
</div>
<GlobalModal dialog={dialog.dialog} onClose={() => (dialog.dialog = null)}></GlobalModal>
