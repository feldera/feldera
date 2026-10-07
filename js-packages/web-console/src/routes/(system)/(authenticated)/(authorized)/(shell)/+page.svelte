<script lang="ts">
  import { Progress } from '@skeletonlabs/skeleton-svelte'
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
  import PinnedSections from '$lib/components/layout/PinnedSections.svelte'
  import BookADemo from '$lib/components/other/BookADemo.svelte'
  import DemoTile from '$lib/components/other/DemoTile.svelte'
  import CreatePipelineButton from '$lib/components/pipelines/CreatePipelineButton.svelte'
  import PipelineTable from '$lib/components/pipelines/Table.svelte'
  import AvailableActions from '$lib/components/pipelines/table/AvailableActions.svelte'
  import OpenSupportBundleDialog from '$lib/components/supportBundle/OpenSupportBundleDialog.svelte'
  import { useAdaptiveDrawer } from '$lib/compositions/layout/useAdaptiveDrawer.svelte'
  import { useGlobalDialog } from '$lib/compositions/layout/useGlobalDialog.svelte'
  import { useIsScreenMd, useIsTablet } from '$lib/compositions/layout/useIsMobile.svelte'
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'
  import { usePipelineList } from '$lib/compositions/pipelines/usePipelineList.svelte'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  import { useDemos } from '$lib/compositions/useDemos.svelte'
  import { resolve } from '$lib/functions/svelte'

  preloadCode(resolve(`/pipelines/*`)).then(() => preloadCode(resolve(`/demos/`)))

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

  const pipelines = usePipelineList()
  const welcomed = useLocalStorage('home/welcomed', false)
  const showSuggestedDemos = useLocalStorage('home/hideSuggestedDemos', true)
  const darkMode = useDarkMode()
  let selectedPipelines = $state([]) as string[]
  const drawer = useAdaptiveDrawer('right')
  // The header's links and buttons fit from 768px; only below that do they move into the
  // drawer (rather than below 1280px, where the drawer itself switches to overlay mode).
  const isScreenMd = useIsScreenMd()
  // The drawer's toggle lives in the collapsed header, so close the drawer once the header
  // expands and the toggle is gone.
  $effect(() => {
    if (isScreenMd.current) {
      drawer.value = false
    }
  })

  const demos = useDemos()
  const globalDialog = useGlobalDialog()
</script>

{#snippet supportBundleDialog()}
  <OpenSupportBundleDialog></OpenSupportBundleDialog>
{/snippet}

<!-- The home page uses 20px side gutters (as the pipeline page) rather than the default 32px. -->
<AppHeader paddingX="px-2 md:px-5">
  {#snippet beforeEnd()}
    {#if !isScreenMd.current}
      <button
        onclick={() => (drawer.value = !drawer.value)}
        class="fd fd-book-open btn-icon flex preset-tonal-surface text-[16px]"
        aria-label="Open the right navigation drawer"
      >
      </button>
    {:else}
      <NavigationExtras></NavigationExtras>
      <button
        class="fd fd-stethoscope btn-icon preset-tonal-surface text-[16px]"
        onclick={() => (globalDialog.dialog = supportBundleDialog)}
        title="Open support bundle"
        data-testid="btn-open-support-bundle"
      >
      </button>
      <BookADemo class="btn preset-filled-primary-500" placement="home">Book a demo</BookADemo>
    {/if}
  {/snippet}
</AppHeader>
<!-- The scrollbar's space is always reserved, so the content keeps one width whether or
     not the page scrolls (expanding the use cases no longer narrows the table), and the
     pipelines toolbar can make up that fixed width to line up with the header. -->
<div
  class="scrollbar flex h-full flex-col justify-between overflow-y-auto [scrollbar-gutter:stable]"
  data-testid="box-home-scroll-area"
>
  <div class="@container">
    <div class="flex flex-col gap-8 pb-2 md:pb-8" style="width: max-content; min-width: 100%;">
      {#if !welcomed.value}
        <!-- The right padding gives back the reserved scrollbar gutter, so the banner ends
             where the header's buttons do (as the pipelines toolbar does). -->
        <div
          class="sticky left-0 max-w-[100cqi] px-2 pt-0 md:pr-[calc(--spacing(5)-var(--scrollbar-width))] md:pl-5"
        >
          <div class="relative flex w-full items-center gap-4 p-6 sm:gap-12">
            <div class="welcome-banner-bg absolute top-0 left-0 -z-10 h-full w-full card"></div>
            <!-- A fixed, whole-pixel size (102 × 70px) close to the logo's 1.4516 ratio. The even
                 height keeps it on whole pixels when centred beside the 76px text. -->
            {#if darkMode.current === 'dark'}
              <FelderaLogomarkDark class="hidden h-[70px] w-[102px] shrink-0 sm:inline"
              ></FelderaLogomarkDark>
            {:else}
              <FelderaLogomarkLight class="hidden h-[70px] w-[102px] shrink-0 sm:inline"
              ></FelderaLogomarkLight>
            {/if}
            <!-- The title (styled as "Your pipelines") with its links right below it, the
                 two centred together beside the logo. -->
            <div class="flex w-full flex-col justify-center gap-y-4">
              <div class="flex flex-nowrap justify-between">
                <div class="text-xl font-semibold">Explore our communities and documentation</div>
                <!-- The negative margins keep the title row's height and the ×'s position. -->
                <button
                  class="fd fd-x -my-0.5 -mr-2 btn-icon text-[16px] hover:preset-tonal-surface"
                  aria-label="Close"
                  onclick={() => (welcomed.value = !welcomed.value)}
                ></button>
              </div>

              <div class="flex flex-col gap-3 lg:flex-row">
                {#each featured as link}
                  <a
                    class="bg-white-dark btn px-6! py-3!"
                    href={link.href}
                    target="_blank"
                    rel="noreferrer"
                    ><link.icon class="h-4 w-4 fill-surface-950-50"></link.icon>{link.title}</a
                  >
                {/each}
              </div>
            </div>
          </div>
        </div>
      {/if}
      <!-- Without the banner, pad the section so it starts 40px below the header logo. -->
      <div class="flex flex-col" class:pt-4={welcomed.value} data-testid="box-pipelines-section">
        {#snippet header()}
          <!-- Raised 2px so its baseline (and lowercase letters) line up with the labels of
               the controls beside it; box-centred, the mostly lowercase title reads low. -->
          <div class="relative -top-0.5 text-xl font-semibold whitespace-nowrap">
            Your pipelines
          </div>
        {/snippet}
        {#if !pipelines.pipelines}
          <div class="flex w-full flex-col items-center gap-4 pt-8 sm:pt-16">
            <Progress class="" value={null}>
              <Progress.Track>
                <Progress.Range class="bg-primary-500" />
              </Progress.Track>
            </Progress>
            <div class="">Loading pipelines...</div>
          </div>
        {:else if pipelines.pipelines.length}
          {@const ps = pipelines.pipelines}
          <PipelineTable pipelines={pipelines.pipelines} bind:selectedPipelines {header}>
            {#snippet preHeaderEnd()}
              <AvailableActions pipelines={ps} bind:selectedPipelines></AvailableActions>
              {#if !selectedPipelines.length}
                <CreatePipelineButton
                  inputClass="max-w-64"
                  btnClass="preset-filled-surface-50-950"
                  shortLabelOnMobile
                ></CreatePipelineButton>
              {/if}
            {/snippet}
          </PipelineTable>
        {:else}
          <div class="px-2 md:px-5">
            {@render header()}
          </div>
          <div class="flex w-full flex-col items-center gap-4 pt-8 sm:pt-16">
            <ImageBox class="h-9 fill-surface-200-800"></ImageBox>
            <div class="">Your pipelines will appear here</div>
            <div class="relative flex gap-5">
              <CreatePipelineButton btnClass="preset-filled-surface-50-950"></CreatePipelineButton>
              <a class="btn preset-tonal-surface" href="https://docs.feldera.com">
                <span class="fd fd-book-open"></span>
                Documentation
              </a>
            </div>
          </div>
        {/if}
      </div>
      {#if demos.current.length}
        <!-- Held at the bottom of the screen while the pipelines table scrolls. -->
        <PinnedSections class="max-w-[100cqi] gap-8">
          <!-- Right padding less the scrollbar gutter, as for the banner above. -->
          <div class="px-2 md:pr-[calc(--spacing(5)-var(--scrollbar-width))] md:pl-5">
            <InlineDropdown bind:open={showSuggestedDemos.value}>
              {#snippet header(open, toggle)}
                <div
                  class="flex w-fit cursor-pointer items-center gap-4 py-2"
                  onclick={(e) => {
                    // Let a click on the "View all" link reach the SvelteKit's router
                    // instead of toggling the dropdown.
                    if ((e.target as HTMLElement).closest('a')) {
                      return
                    }
                    toggle()
                  }}
                  role="presentation"
                >
                  <div
                    class={'fd fd-chevron-down text-xl transition-transform ' +
                      (open ? 'rotate-180' : '')}
                  ></div>

                  <div class="flex flex-nowrap items-center gap-4">
                    <div class="text-xl font-semibold">Explore use cases and tutorials</div>
                    <a class="whitespace-nowrap text-primary-500" href={resolve('/demos/')}
                      >View all</a
                    >
                  </div>
                </div>
              {/snippet}
              {#snippet content()}
                <div
                  transition:slide={{ duration: 150 }}
                  class="grid grid-cols-1 gap-x-6 gap-y-5 py-2 sm:grid-cols-2 lg:grid-cols-3 2xl:grid-cols-5"
                >
                  {#each demos.current.slice(0, maxShownDemos) as demo}
                    <DemoTile {demo} placement="home"></DemoTile>
                  {/each}
                  <div class="flex flex-col card p-4">
                    <div class="text-sm text-surface-500"></div>
                    <a class="text-left text-primary-500" href={resolve('/demos/')}>
                      <span class="py-2">Discover More Examples and Tutorials</span>
                    </a>
                  </div>
                </div>
              {/snippet}
            </InlineDropdown>
          </div>
        </PinnedSections>
      {/if}
    </div>
  </div>
  <div class="sticky left-0"><Footer></Footer></div>
</div>
