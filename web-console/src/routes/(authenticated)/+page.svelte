<script lang="ts">
  import { preloadCode } from '$app/navigation'
  import { base } from '$app/paths'
  import { useIsMobile, useIsTablet } from '$lib/compositions/layout/useIsMobile.svelte'
  import FelderaLogomarkLight from '$assets/images/feldera-modern/Feldera Logomark Color Dark.svg?component'
  import FelderaLogomarkDark from '$assets/images/feldera-modern/Feldera Logomark Color Light.svg?component'
  import IconBook from '$assets/icons/feldera-material-icons/book-marked.svg?component'
  import IconDiscord from '$assets/icons/vendors/discord-logomark-color.svg?component'
  import ImageBox from '$assets/images/generic/package.svg?component'
  import IconSlack from '$assets/icons/vendors/slack-logomark-color.svg?component'
  import InlineDropdown from '$lib/components/common/InlineDropdown.svelte'
  import { slide } from 'svelte/transition'
  import { usePipelineList } from '$lib/compositions/pipelines/usePipelineList.svelte'
  import type { PageData } from './$types'
  import PipelineTable from '$lib/components/pipelines/Table.svelte'
  import DemoTile from '$lib/components/other/DemoTile.svelte'
  import CreatePipelineButton from '$lib/components/pipelines/CreatePipelineButton.svelte'
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  import AvailableActions from '$lib/components/pipelines/table/AvailableActions.svelte'
  import AppHeader from '$lib/components/layout/AppHeader.svelte'
  import NavigationExtras from '$lib/components/layout/NavigationExtras.svelte'
  import { useDrawer } from '$lib/compositions/layout/useDrawer.svelte'

  preloadCode(`${base}/pipelines/*`).then(() => preloadCode(`${base}/demos/`))

  let { data }: { data: PageData } = $props()
  const isMobile = useIsMobile()
  const isTablet = useIsTablet()

  const featured = [
    {
      title: 'Documentation',
      href: 'https://docs.feldera.com',
      icon: IconBook
    },
    {
      title: 'Join the Conversation',
      href: 'https://felderacommunity.slack.com/join/shared_invite/zt-222bq930h-dgsu5IEzAihHg8nQt~dHzA',
      icon: IconSlack
    },
    {
      title: 'Join the Community',
      href: 'https://discord.com/invite/s6t5n9UzHE',
      icon: IconDiscord
    }
  ]

  const maxShownDemos = $derived(isMobile.current ? 3 : isTablet.current ? 5 : 9)

  const pipelines = usePipelineList(data.preloaded)
  let welcomed = useLocalStorage('home/welcomed', false)
  let showSuggestedDemos = useLocalStorage('home/hideSuggestedDemos', true)
  let darkMode = useDarkMode()
  let selectedPipelines = $state([]) as string[]
  const drawer = useDrawer('right')
</script>

<AppHeader>
  {#snippet beforeEnd()}
    {#if drawer.isMobileDrawer}
      <button
        onclick={() => (drawer.value = !drawer.value)}
        class="fd fd-menu btn-icon flex text-[24px] preset-tonal-surface"
        aria-label="Open extras drawer"
      >
      </button>
    {:else}
      <NavigationExtras></NavigationExtras>
      <div class="relative">
        <CreatePipelineButton class="max-w-64" btnClass="preset-filled-surface-50-950"
        ></CreatePipelineButton>
      </div>
    {/if}
  {/snippet}
</AppHeader>
<div class="h-full overflow-y-auto scrollbar">
  <div class="flex flex-col gap-8 p-2 pt-0 md:p-8 md:pt-0">
    {#if !welcomed.value}
      <div class="relative flex min-h-40 w-full gap-4 p-6 sm:gap-12">
        <div class="card absolute left-0 top-0 -z-10 flex h-full w-full overflow-clip">
          <div
            class="w-1/2 bg-gradient-to-br from-fuchsia-300 via-amber-50 to-orange-300 dark:from-fuchsia-700 dark:via-amber-950 dark:to-orange-700"
          ></div>
          <div
            class="w-1/2 bg-gradient-to-tr from-orange-300 via-amber-50 to-amber-50 dark:from-orange-700 dark:via-amber-950 dark:to-amber-950"
          ></div>
        </div>
        {#if darkMode.current === 'dark'}
          <FelderaLogomarkDark class="hidden h-full max-h-28 sm:inline"></FelderaLogomarkDark>
        {:else}
          <FelderaLogomarkLight class="hidden h-full max-h-28 sm:inline"></FelderaLogomarkLight>
        {/if}
        <div class="flex w-full flex-col justify-between gap-y-2">
          <div class="flex flex-nowrap justify-between">
            <div class="h5">Explore our communities and documentation</div>
            <button
              class="fd fd-x btn-icon-lg"
              aria-label="Close"
              onclick={() => (welcomed.value = !welcomed.value)}
            ></button>
          </div>

          <div class="flex flex-col gap-x-8 gap-y-4 lg:flex-row">
            {#each featured as link}
              <a class="bg-white-black btn !p-6" href={link.href} target="_blank" rel="noreferrer"
                ><link.icon class="h-6 w-6"></link.icon>{link.title}</a
              >
            {/each}
          </div>
        </div>
      </div>
    {/if}

    {#if data.demos.length}
      <div>
        <InlineDropdown bind:open={showSuggestedDemos.value}>
          {#snippet header(open, toggle)}
            <div
              class="flex w-fit cursor-pointer items-center gap-2 py-2"
              onclick={toggle}
              role="presentation"
            >
              <div
                class={'fd fd-chevron-down text-[24px] transition-transform ' +
                  (open ? 'rotate-180' : '')}
              ></div>

              <div class="flex flex-nowrap items-center gap-4">
                <div class="h5">Explore use cases and tutorials</div>
                <a
                  class="whitespace-nowrap text-primary-500"
                  href="{base}/demos/"
                  onclick={(e) => e.stopPropagation()}>View all</a
                >
              </div>
            </div>
          {/snippet}
          {#snippet content()}
            <div transition:slide={{ duration: 150 }}>
              <div
                class="grid grid-cols-1 gap-x-6 gap-y-5 py-2 sm:grid-cols-2 lg:grid-cols-3 2xl:grid-cols-5"
              >
                {#each data.demos.slice(0, maxShownDemos) as demo}
                  <DemoTile {demo}></DemoTile>
                {/each}
                <div class="card flex flex-col p-4">
                  <div class="text-sm text-surface-500">&nbsp;</div>
                  <a class="text-left text-primary-500" href="{base}/demos/">
                    <span class="py-2 text-lg">Discover More Examples and Tutorials</span>
                    <!-- <span class="fd fd-arrow-right inline-block w-2 text-[24px]"></span> -->
                  </a>
                </div>
              </div>
            </div>
          {/snippet}
        </InlineDropdown>
      </div>
    {/if}
    <div class="min-h-96">
      <div class="h5 flex flex-nowrap items-center gap-4">
        <span class="fd fd-network text-surface-500"></span><span>Your pipelines</span>
      </div>
      {#if pipelines.pipelines.length}
        <PipelineTable pipelines={pipelines.pipelines} bind:selectedPipelines>
          {#snippet preHeaderEnd()}
            {#if selectedPipelines.length}
              <AvailableActions pipelines={pipelines.pipelines} bind:selectedPipelines
              ></AvailableActions>
            {:else}
              <CreatePipelineButton class="max-w-64"></CreatePipelineButton>
            {/if}
          {/snippet}
        </PipelineTable>
      {:else}
        <div class="flex w-full flex-col items-center gap-4 pt-8 sm:pt-16">
          <ImageBox class="h-9 fill-surface-200-800"></ImageBox>
          <div class="text-lg">Your pipelines will appear here</div>
          <div class="relative flex gap-5">
            <CreatePipelineButton></CreatePipelineButton>
            <a class="btn text-sm preset-tonal-surface" href="https://docs.feldera.com">
              <span class="fd fd-book-marked text-2xl"></span>
              Documentation
            </a>
          </div>
        </div>
      {/if}
    </div>
  </div>
</div>
