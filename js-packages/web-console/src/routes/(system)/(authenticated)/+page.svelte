<script lang="ts">
  import { slide } from 'svelte/transition'
  import { preloadCode } from '$app/navigation'
  import IconBookOpen from '$assets/icons/feldera-material-icons/book-open.svg?component'
  import IconDiscord from '$assets/icons/vendors/discord-logomark-color.svg?component'
  import IconSlack from '$assets/icons/vendors/slack-logomark-color.svg?component'
  import FelderaLogomarkLight from '$assets/images/feldera-modern/Feldera Logomark Color Dark.svg?component'
  import FelderaLogomarkDark from '$assets/images/feldera-modern/Feldera Logomark Color Light.svg?component'
  import ImageBox from '$assets/images/generic/package.svg?component'
  import InlineDropdown from '$lib/components/common/InlineDropdown.svelte'
  import AppHeader from '$lib/components/layout/AppHeader.svelte'
  import Footer from '$lib/components/layout/Footer.svelte'
  import NavigationExtras from '$lib/components/layout/NavigationExtras.svelte'
  import BookADemo from '$lib/components/other/BookADemo.svelte'
  import DemoTile from '$lib/components/other/DemoTile.svelte'
  import CreatePipelineButton from '$lib/components/pipelines/CreatePipelineButton.svelte'
  import PipelineTable from '$lib/components/pipelines/Table.svelte'
  import AvailableActions from '$lib/components/pipelines/table/AvailableActions.svelte'
  import { useAdaptiveDrawer } from '$lib/compositions/layout/useAdaptiveDrawer.svelte'
  import { useIsTablet } from '$lib/compositions/layout/useIsMobile.svelte'
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'
  import { usePipelineList } from '$lib/compositions/pipelines/usePipelineList.svelte'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  import { resolve } from '$lib/functions/svelte'
  import type { PageData } from './$types'

  preloadCode(resolve(`/pipelines/*`)).then(() => preloadCode(resolve(`/demos/`)))

  const { data }: { data: PageData } = $props()
  const isTablet = useIsTablet()

  const featured = [
    {
      title: 'Documentation',
      href: 'https://docs.feldera.com',
      icon: IconBookOpen
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

  const maxShownDemos = $derived(isTablet.current ? 5 : 9)

  const pipelines = usePipelineList(data.preloaded)
  const welcomed = useLocalStorage('home/welcomed', false)
  const showSuggestedDemos = useLocalStorage('home/hideSuggestedDemos', true)
  const darkMode = useDarkMode()
  let selectedPipelines = $state([]) as string[]
  const drawer = useAdaptiveDrawer('right')
</script>

<AppHeader>
  {#snippet beforeEnd()}
    {#if drawer.isMobileDrawer}
      <button
        onclick={() => (drawer.value = !drawer.value)}
        class="fd fd-book-open btn-icon flex preset-tonal-surface text-[20px]"
        aria-label="Open the right navigation drawer"
      >
      </button>
    {:else}
      <NavigationExtras></NavigationExtras>
      <div class="relative">
        <CreatePipelineButton inputClass="max-w-64" btnClass="preset-filled-surface-50-950"
        ></CreatePipelineButton>
      </div>
      <BookADemo class="btn preset-filled-primary-500">Book a demo</BookADemo>
    {/if}
  {/snippet}
</AppHeader>
<div class="scrollbar flex h-full flex-col justify-between overflow-y-auto">
  <div class="flex flex-col gap-8 p-2 pt-0 md:p-8 md:pt-0">
    {#if !welcomed.value}
      <div class="relative flex min-h-40 w-full gap-4 p-6 sm:gap-12">
        <div class="absolute top-0 left-0 -z-10 flex h-full w-full overflow-clip card">
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
        <div class="flex w-full flex-col justify-between gap-y-4">
          <div class="flex flex-nowrap justify-between">
            <div class="text-2xl font-semibold">Explore our communities and documentation</div>
            <button
              class="fd fd-x w-7 text-[24px]"
              aria-label="Close"
              onclick={() => (welcomed.value = !welcomed.value)}
            ></button>
          </div>

          <div class="flex flex-col gap-x-8 gap-y-4 lg:flex-row">
            {#each featured as link}
              <a
                class="bg-white-dark btn px-6! py-3!"
                href={link.href}
                target="_blank"
                rel="noreferrer"
                ><link.icon class="h-6 w-6 fill-surface-950-50"></link.icon>{link.title}</a
              >
            {/each}
          </div>
        </div>
      </div>
    {/if}
    <div class="flex flex-col">
      <div class="flex flex-nowrap items-center gap-4 text-xl font-semibold">
        <span class="fd fd-network text-surface-500"></span><span>Your pipelines</span>
      </div>
      {#if pipelines.pipelines.length}
        <PipelineTable pipelines={pipelines.pipelines} bind:selectedPipelines>
          {#snippet preHeaderEnd()}
            <AvailableActions pipelines={pipelines.pipelines} bind:selectedPipelines
            ></AvailableActions>
            {#if !selectedPipelines.length}
              <CreatePipelineButton
                inputClass="max-w-64"
                btnClass="hidden sm:flex preset-filled-surface-50-950"
              ></CreatePipelineButton>
            {/if}
          {/snippet}
        </PipelineTable>
      {:else}
        <div class="flex w-full flex-col items-center gap-4 pt-8 sm:pt-16">
          <ImageBox class="h-9 fill-surface-200-800"></ImageBox>
          <div class="">Your pipelines will appear here</div>
          <div class="relative flex gap-5">
            <CreatePipelineButton btnClass="preset-filled-surface-50-950"></CreatePipelineButton>
            <a class="btn preset-tonal-surface" href="https://docs.feldera.com">
              <span class="fd fd-book-open text-2xl"></span>
              Documentation
            </a>
          </div>
        </div>
      {/if}
    </div>
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
                class={'fd fd-chevron-down text-[20px] transition-transform ' +
                  (open ? 'rotate-180' : '')}
              ></div>

              <div class="flex flex-nowrap items-center gap-4">
                <div class="text-xl font-semibold">Explore use cases and tutorials</div>
                <a
                  class="whitespace-nowrap text-primary-500"
                  href={resolve('/demos/')}
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
                <div class="flex flex-col card p-4">
                  <div class="text-sm text-surface-500"></div>
                  <a class="text-left text-primary-500" href={resolve('/demos/')}>
                    <span class="py-2">Discover More Examples and Tutorials</span>
                    <!-- <span class="fd fd-arrow-right inline-block w-2 text-[20px]"></span> -->
                  </a>
                </div>
              </div>
            </div>
          {/snippet}
        </InlineDropdown>
      </div>
    {/if}
  </div>
  <Footer></Footer>
</div>
