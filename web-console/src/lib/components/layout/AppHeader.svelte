<script lang="ts">
  import GlobalModal from '$lib/components/dialogs/GlobalModal.svelte'
  import FelderaModernLogoColorDark from '$assets/images/feldera-modern/Feldera Logo Color Dark.svg?component'
  import FelderaModernLogoColorLight from '$assets/images/feldera-modern/Feldera Logo Color Light.svg?component'
  import FelderaModernLogomarkColorDark from '$assets/images/feldera-modern/Feldera Logomark Color Dark.svg?component'
  import FelderaModernLogomarkColorLight from '$assets/images/feldera-modern/Feldera Logomark Color Light.svg?component'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  import CreatePipelineButton from '$lib/components/pipelines/CreatePipelineButton.svelte'
  import type { Snippet } from 'svelte'
  import { useGlobalDialog } from '$lib/compositions/useGlobalDialog.svelte'
  import AuthButton from '$lib/components/auth/AuthButton.svelte'
  import { useRefreshPipelineList } from '$lib/compositions/pipelines/usePipelineList.svelte'
  import { base } from '$app/paths'
  import { useDrawer } from '$lib/compositions/layout/useDrawer.svelte'
  import Popup from '$lib/components/common/Popup.svelte'
  import { fade } from 'svelte/transition'
  import { page } from '$app/stores'
  import { fromStore } from 'svelte/store'
  import NavigationExtras from './NavigationExtras.svelte'

  const dialog = useGlobalDialog()

  let { children }: { children?: Snippet } = $props()
  let { darkMode, toggleDarkMode } = useDarkMode()
  let showDrawer = useDrawer()

  useRefreshPipelineList()

  const drawer = useDrawer()
</script>

<div class="flex w-full flex-row items-center justify-between gap-4 px-2 py-2 md:px-8">
  <a class="py-2 sm:pb-6 sm:pr-6 sm:pt-0" href="{base}/">
    <span class="hidden sm:block">
      {#if darkMode.value === 'dark'}
        <FelderaModernLogoColorLight class="h-12"></FelderaModernLogoColorLight>
      {:else}
        <FelderaModernLogoColorDark class="h-12"></FelderaModernLogoColorDark>
      {/if}
    </span>
    <span class="inline sm:hidden">
      {#if darkMode.value === 'dark'}
        <FelderaModernLogomarkColorLight class="h-8"></FelderaModernLogomarkColorLight>
      {:else}
        <FelderaModernLogomarkColorDark class="h-8"></FelderaModernLogomarkColorDark>
      {/if}
    </span>
  </a>
  {@render children?.()}
  <!-- <div class="flex flex-1"></div> -->
  <div class="-mr-4 ml-auto"></div>
  {#if drawer.isMobileDrawer}
    <button
      onclick={() => (drawer.value = !drawer.value)}
      class="fd fd-menu btn-icon flex text-[24px] preset-tonal-surface xl:hidden"
      aria-label="Pipelines list"
    >
    </button>
    <AuthButton compactBreakpoint="xl:"></AuthButton>
  {:else}
    <div class="hidden h-9 gap-2 xl:flex">
      <div class="relative">
        <CreatePipelineButton></CreatePipelineButton>
      </div>
      <NavigationExtras></NavigationExtras>
      <AuthButton compactBreakpoint="xl:"></AuthButton>
    </div>
  {/if}
</div>
