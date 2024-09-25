<script lang="ts">
  import Drawer from '$lib/components/layout/Drawer.svelte'
  import GlobalModal from '$lib/components/dialogs/GlobalModal.svelte'
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'
  import FelderaModernLogoColorDark from '$assets/images/feldera-modern/Feldera Logo Color Dark.svg?component'
  import FelderaModernLogoColorLight from '$assets/images/feldera-modern/Feldera Logo Color Light.svg?component'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  import PipelinesList from '$lib/components/pipelines/List.svelte'
  import type { Snippet } from 'svelte'
  import { navItems } from '$lib/functions/navigation/items'
  import { useGlobalDialog } from '$lib/compositions/useGlobalDialog.svelte'
  import AuthButton from '$lib/components/auth/AuthButton.svelte'
  import type { LayoutData } from './$types'
  import { usePipelineList } from '$lib/compositions/pipelines/usePipelineList.svelte'
  import { base } from '$app/paths'
  import { page } from '$app/stores'
  import { SvelteKitTopLoader } from 'sveltekit-top-loader'

  const dialog = useGlobalDialog()

  let { children, data }: { children: Snippet; data: LayoutData } = $props()
  let { darkMode, toggleDarkMode } = useDarkMode()
  let showDrawer = useLocalStorage('layout/drawer', true)

  let pipelines = usePipelineList(data.preloaded)
</script>

<SvelteKitTopLoader height={2} color={'rgb(var(--color-primary-500))'} showSpinner={false}
></SvelteKitTopLoader>
<div class="flex h-full">
  <Drawer width="w-[22rem]" bind:open={showDrawer.value} side="left">
    <div class="flex h-full w-full flex-col gap-1">
      <span class="mx-5 my-4 flex items-end justify-center">
        <a href="{base}/">
          {#if darkMode.value === 'dark'}
            <FelderaModernLogoColorLight class="h-12"></FelderaModernLogoColorLight>
          {:else}
            <FelderaModernLogoColorDark class="h-12"></FelderaModernLogoColorDark>
          {/if}
        </a>
      </span>
      <PipelinesList bind:pipelines={pipelines.pipelines}></PipelinesList>
      <span class="text-surface-700-300 mt-auto pb-1 pl-4">{$page.data.felderaVersion}</span>
    </div>
  </Drawer>
  <div class="flex h-full w-full flex-col">
    <div class="flex justify-between p-1">
      <div class="flex">
        <button
          class="btn-icon"
          onclick={() => {
            showDrawer.value = !showDrawer.value
          }}>
          <i class="fd fd-menu text-[24px]"></i>
        </button>
      </div>
      <div class="flex"></div>
      <div class="flex gap-2">
        {#each navItems({ showSettings: false }) as item}
          <a
            href={Array.isArray(item.path) ? item.path[0] : item.path}
            class="preset-grayout-surface flex flex-nowrap items-center justify-center"
            {...item.openInNewTab ? { target: '_blank', rel: 'noreferrer' } : undefined}>
            <div class="flex w-9 justify-center">
              <div class={item.class + ' text-[24px]'}></div>
            </div>
            <span class="hidden xl:block">{item.title}</span>
          </a>
        {/each}
        <!-- <HealthPopup></HealthPopup> -->
        <button
          onclick={toggleDarkMode}
          class="btn-icon preset-tonal-surface text-[24px]
            {darkMode.value === 'dark' ? 'fd fd-brightness_5' : 'fd fd-brightness_2'}"></button>
        <AuthButton compactBreakpoint="xl:"></AuthButton>
      </div>
    </div>
    {@render children()}
  </div>
</div>
<GlobalModal dialog={dialog.dialog} onClose={() => (dialog.dialog = null)}></GlobalModal>
