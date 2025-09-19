<script lang="ts">
  import GlobalModal from '$lib/components/dialogs/GlobalModal.svelte'
  import type { Snippet } from 'svelte'
  import { useGlobalDialog } from '$lib/compositions/useGlobalDialog.svelte'
  import type { LayoutData } from './$types'
  import {
    usePipelineList,
    useRefreshPipelineList
  } from '$lib/compositions/pipelines/usePipelineList.svelte'
  import SvelteKitTopLoader from '$lib/components/common/SvelteKitTopLoader.svelte'
  import { useDrawer } from '$lib/compositions/layout/useDrawer.svelte'
  import ModalDrawer from '$lib/components/layout/ModalDrawer.svelte'
  import NavigationExtras from '$lib/components/layout/NavigationExtras.svelte'
  import CreatePipelineButton from '$lib/components/pipelines/CreatePipelineButton.svelte'
  import PipelineList from '$lib/components/pipelines/List.svelte'
  import BookADemo from '$lib/components/other/BookADemo.svelte'
  import { useLayoutSettings } from '$lib/compositions/layout/useLayoutSettings.svelte'
  import { useIsTablet } from '$lib/compositions/layout/useIsMobile.svelte'
  import LineBanner, { BannerButton } from '$lib/components/layout/LineBanner.svelte'
  import { useSystemMessages } from '$lib/compositions/useSystemMessages.svelte'
  import { useInterval } from '$lib/compositions/common/useInterval.svelte'
  import Dayjs from 'dayjs'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import { useToast } from '$lib/compositions/useToastNotification'
  import { ServiceWorkerMarkers } from '$lib/types/serviceWorker'

  const dialog = useGlobalDialog()

  let { children, data }: { children: Snippet; data: LayoutData } = $props()

  useRefreshPipelineList()
  const rightDrawer = useDrawer('right')
  const isTablet = useIsTablet()
  const { showPipelinesPanel: leftDrawer } = useLayoutSettings()
  const pipelineList = usePipelineList(data.preloaded)

  const systemMessages = useSystemMessages()
  const now = useInterval(() => new Date(), 3600000, 3600000 - (Date.now() % 3600000))
  const displayedMessages = $derived(
    systemMessages.displayedMessages.map((message) => {
      const text = message.text.replace(/\{toDaysHoursFromNow (\d+)\}/, (_match, milliseconds) => {
        const duration = Dayjs.duration(
          parseInt(milliseconds) - now.current.valueOf(),
          'milliseconds'
        )
        const days = duration.asDays()
        const hours = duration.hours()
        return days > 1
          ? `${days.toFixed()} ${'days'}`
          : hours > 0
            ? `${hours.toFixed()} ${hours > 1 ? 'hours' : 'hour'}`
            : 'less than an hour'
      })
      return { ...message, text }
    })
  )

  let api = usePipelineManager()
  let { toastMain, dismissMain } = useToast()
  $effect(() => {
    if (api.isNetworkHealthy) {
      dismissMain()
    } else {
      toastMain(
        'Unable to reach this Feldera instance.\nEither the cluster is unresponsive, or there is an issue with the network connection.'
      )
    }
  })
</script>

<SvelteKitTopLoader
  height={2}
  color={'rgb(var(--color-primary-500))'}
  showSpinner={false}
  ignoreBeforeNavigate={({ to }) =>
    to?.url?.searchParams?.has(ServiceWorkerMarkers.ADD_AUTH_HEADER) ?? false}
  ignoreAfterNavigate={({ to }) =>
    to?.url?.searchParams?.has(ServiceWorkerMarkers.ADD_AUTH_HEADER) ?? false}
></SvelteKitTopLoader>
<div
  class="flex h-full w-full flex-col {api.isNetworkHealthy
    ? ''
    : 'disabled pointer-events-auto select-text [&_.monaco-editor-background]:pointer-events-none [&_[role="button"]]:pointer-events-none [&_[role="separator"]]:pointer-events-none [&_a]:pointer-events-none [&_button]:pointer-events-none'}"
  style={api.isNetworkHealthy ? '' : ''}
>
  {#each displayedMessages as message}
    {#if message.id.startsWith('expiring_license_')}
      <LineBanner
        dismiss={message.dismissable !== 'never'
          ? () => systemMessages.dismiss(message.id)
          : undefined}
      >
        {#snippet start()}
          <span>{@html message.text}</span>
          {#if message.action}
            {@render BannerButton(message.action)}
          {/if}
        {/snippet}
      </LineBanner>
    {:else if message.id.startsWith('version_available_')}
      <LineBanner
        dismiss={message.dismissable !== 'never'
          ? () => systemMessages.dismiss(message.id)
          : undefined}
        variant="aether"
      >
        {#snippet center()}
          <span>{@html message.text}</span>
          {#if message.action}
            {@render BannerButton(message.action)}
          {/if}
        {/snippet}
      </LineBanner>
    {:else}
      <LineBanner
        dismiss={message.dismissable !== 'never'
          ? () => systemMessages.dismiss(message.id)
          : undefined}
      >
        {#snippet start()}
          <span>{@html message.text}</span>
          {#if message.action}
            {@render BannerButton(message.action)}
          {/if}
        {/snippet}
      </LineBanner>
    {/if}
  {/each}
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
    {@render children()}
  </div>
  {#if isTablet.current}
    <ModalDrawer
      width="w-72"
      bind:open={leftDrawer.value}
      side="left"
      class="bg-white-dark flex flex-col gap-2 pl-4 pr-1 pt-8"
    >
      <PipelineList
        pipelines={pipelineList.pipelines}
        onclose={() => {
          leftDrawer.value = false
        }}
        onaction={() => {
          leftDrawer.value = false
        }}
      ></PipelineList>
    </ModalDrawer>
  {/if}
  <ModalDrawer
    width="w-72"
    bind:open={rightDrawer.value}
    side="right"
    class="bg-white-dark flex flex-col gap-2 p-4"
  >
    <div class="relative my-2 mt-4">
      <CreatePipelineButton
        btnClass="preset-filled-surface-50-950"
        onSuccess={() => {
          rightDrawer.value = false
        }}
      ></CreatePipelineButton>
    </div>
    <BookADemo class="self-center preset-filled-primary-500">Book a demo</BookADemo>
    <NavigationExtras inline></NavigationExtras>
  </ModalDrawer>
</div>
<GlobalModal dialog={dialog.dialog} onClose={() => (dialog.dialog = null)}></GlobalModal>
