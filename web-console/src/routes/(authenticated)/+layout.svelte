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
  import type { LayoutData } from './$types'
  import { useRefreshPipelineList } from '$lib/compositions/pipelines/usePipelineList.svelte'
  import { base } from '$app/paths'
  import { SvelteKitTopLoader } from 'sveltekit-top-loader'
  import { useDrawer } from '$lib/compositions/layout/useDrawer.svelte'
  import Popup from '$lib/components/common/Popup.svelte'
  import { fade } from 'svelte/transition'
  import { page } from '$app/stores'
  import { fromStore } from 'svelte/store'

  const dialog = useGlobalDialog()

  let { children, data }: { children: Snippet; data: LayoutData } = $props()
  let { darkMode, toggleDarkMode } = useDarkMode()
  let showDrawer = useDrawer()

  useRefreshPipelineList()
  const communityResources = [
    {
      title: 'Discord',
      path: 'https://discord.com/invite/s6t5n9UzHE',
      class: 'font-brands fa-discord w-6  before:-ml-0.5',
      openInNewTab: true,
      testid: 'button-vertical-nav-discord'
    },
    {
      title: 'Slack',
      path: 'https://felderacommunity.slack.com',
      class: 'font-brands fa-slack w-6 before:ml-0.5',
      openInNewTab: true,
      testid: 'button-vertical-nav-slack'
    }
  ]
  const docChapters = [
    {
      title: 'SQL Reference',
      href: 'https://docs.feldera.com/sql/types'
    },
    {
      title: 'Connectors',
      href: 'https://docs.feldera.com/connectors/sources/'
    },
    {
      title: 'UDFs',
      href: 'https://docs.feldera.com/sql/udf'
    },
    {
      title: 'Feldera 101',
      href: 'https://docs.feldera.com/tutorials/basics/part1'
    }
  ]

  const _page = fromStore(page)
</script>

<SvelteKitTopLoader height={2} color={'rgb(var(--color-primary-500))'} showSpinner={false}
></SvelteKitTopLoader>
<div class="flex h-full">
  <!-- <Drawer width="w-[22rem]" bind:open={showDrawer.value} side="left">
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
    </div>
  </Drawer> -->
  <div class="flex h-full w-full flex-col">
    <div class="flex items-end justify-between px-2 py-2">
      <a class="px-8" href="{base}/">
        <span class="hidden sm:block">
          {#if darkMode.value === 'dark'}
            <FelderaModernLogoColorLight class="h-12"></FelderaModernLogoColorLight>
          {:else}
            <FelderaModernLogoColorDark class="h-12"></FelderaModernLogoColorDark>
          {/if}
        </span>
        <span class="inline sm:hidden">
          {#if darkMode.value === 'dark'}
            <FelderaModernLogomarkColorLight class="h-9"></FelderaModernLogomarkColorLight>
          {:else}
            <FelderaModernLogomarkColorDark class="h-9"></FelderaModernLogomarkColorDark>
          {/if}
        </span>
      </a>
      <div class="flex"></div>
      <div class="flex h-9 gap-2">
        {#if _page.current.url.pathname !== `${base}/`}
          <div class="relative">
            <CreatePipelineButton></CreatePipelineButton>
          </div>
        {/if}
        <Popup>
          {#snippet trigger(toggle)}
            <button onclick={toggle} class="btn">
              <span class="hidden sm:inline">Documentation</span>
              <span class="inline sm:hidden">Docs</span>
            </button>
          {/snippet}
          {#snippet content(close)}
            <div
              transition:fade={{ duration: 100 }}
              class="absolute left-0 z-10 flex max-h-[400px] w-[calc(100vw-100px)] max-w-[200px] flex-col justify-end gap-2 overflow-y-auto rounded bg-white p-2 shadow-md dark:bg-black"
            >
              {#each docChapters as doc}
                <a
                  href={doc.href}
                  target="_blank"
                  rel="noreferrer"
                  class="rounded p-2 hover:preset-tonal-surface">{doc.title}</a
                >
              {/each}
            </div>
          {/snippet}
        </Popup>
        <Popup>
          {#snippet trigger(toggle)}
            <button onclick={toggle} class="btn">
              <span class="hidden sm:inline">Community resources</span>
              <span class="inline sm:hidden">Community</span>
            </button>
          {/snippet}
          {#snippet content(close)}
            <div
              transition:fade={{ duration: 100 }}
              class="absolute right-0 z-10 max-h-[400px] w-[calc(100vw-100px)] max-w-[200px] justify-end overflow-y-auto rounded bg-white shadow-md dark:bg-black"
            >
              {#each communityResources as item}
                <a
                  href={Array.isArray(item.path) ? item.path[0] : item.path}
                  target="_blank"
                  rel="noreferrer"
                  class="preset-grayout-surface flex flex-nowrap items-center p-2"
                  {...item.openInNewTab ? { target: '_blank', rel: 'noreferrer' } : undefined}
                >
                  <div class="flex w-9 justify-center">
                    <div class={item.class + ' text-[24px]'}></div>
                  </div>
                  <span class="">{item.title}</span>
                </a>
              {/each}
            </div>
          {/snippet}
        </Popup>
        <AuthButton compactBreakpoint="xl:"></AuthButton>
      </div>
    </div>
    {@render children()}
  </div>
</div>
<GlobalModal dialog={dialog.dialog} onClose={() => (dialog.dialog = null)}></GlobalModal>
