<script lang="ts">
  import Drawer from '$lib/components/layout/Drawer.svelte'
  import GlobalModal from '$lib/components/dialogs/GlobalModal.svelte'
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'
  import FelderaClassicLogoColor from '$assets/images/feldera-classic/LogoSolid.svg?component'
  import FelderaClassicLogoWhite from '$assets/images/feldera-classic/LogoWhite.svg?component'
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
  import { useSkeletonTheme } from '$lib/compositions/useSkeletonTheme.svelte'

  const dialog = useGlobalDialog()

  let { children, data }: { children: Snippet; data: LayoutData } = $props()
  let { darkMode, toggleDarkMode } = useDarkMode()
  let showDrawer = useLocalStorage('layout/drawer', true)

  let pipelines = usePipelineList(data.preloaded)
  let theme = useSkeletonTheme()
</script>

<div class="flex h-full">
  <Drawer width="w-[22rem]" bind:open={showDrawer.value} side="left">
    <div class="flex h-full w-full flex-col gap-1">
      <span class="flex items-end">
        <a href="{base}/">
          {#if theme.current === 'feldera-modern-theme'}
            {#if darkMode.value === 'dark'}
              <FelderaModernLogoColorLight class="w-32 p-3"></FelderaModernLogoColorLight>
            {:else}
              <FelderaModernLogoColorDark class="w-32 p-3"></FelderaModernLogoColorDark>
            {/if}
          {:else if darkMode.value === 'dark'}
            <FelderaClassicLogoWhite class="w-40 p-3"></FelderaClassicLogoWhite>
          {:else}
            <FelderaClassicLogoColor class="w-40 p-3"></FelderaClassicLogoColor>
          {/if}
        </a>
        <span class="pb-1 text-surface-600-400">{$page.data.felderaVersion}</span>
      </span>
      <PipelinesList bind:pipelines={pipelines.pipelines}></PipelinesList>
    </div>
  </Drawer>
  <div class="flex h-full w-full flex-col">
    <div class="flex justify-between p-1">
      <div class="flex">
        <button
          class="btn-icon"
          onclick={() => {
            showDrawer.value = !showDrawer.value
          }}
        >
          <i class="bx bx-menu text-[24px]"></i>
        </button>
      </div>
      <div class="flex"></div>
      <div class="flex gap-2">
        {#each navItems({ showSettings: false }) as item}
          <a
            href={Array.isArray(item.path) ? item.path[0] : item.path}
            class="preset-grayout-surface flex flex-nowrap items-center justify-center"
            {...item.openInNewTab ? { target: '_blank', rel: 'noreferrer' } : undefined}
          >
            <div class="flex w-9 justify-center">
              <div class={item.class + ' text-[24px]'}></div>
            </div>
            <span class="hidden xl:block">{item.title}</span>
          </a>
        {/each}
        <!-- <HealthPopup></HealthPopup> -->
        <button
          onclick={toggleDarkMode}
          class="btn-icon text-[24px] preset-tonal-surface
            {darkMode.value === 'dark' ? 'bx bx-sun' : 'bx bx-moon'}"
        ></button>
        <button
          class="fd fd-swatch-book text-[24px]"
          onclick={() => {
            theme.current =
              theme.current === 'feldera-modern-theme'
                ? 'feldera-classic-theme'
                : 'feldera-modern-theme'
          }}
        >
        </button>
        <AuthButton compactBreakpoint="xl:"></AuthButton>
      </div>
    </div>
    {@render children()}
  </div>
</div>
<GlobalModal dialog={dialog.dialog} onClose={() => (dialog.dialog = null)}></GlobalModal>
