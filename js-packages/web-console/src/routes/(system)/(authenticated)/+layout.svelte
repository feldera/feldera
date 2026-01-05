<script lang="ts">
  import Dayjs from 'dayjs'
  import { invalidateAll } from '$app/navigation'
  import { page } from '$app/state'
  import SvelteKitTopLoader from '$lib/components/common/SvelteKitTopLoader.svelte'
  import GlobalModal from '$lib/components/dialogs/GlobalModal.svelte'
  import LineBanner, { BannerButton } from '$lib/components/layout/LineBanner.svelte'
  import NavigationExtras from '$lib/components/layout/NavigationExtras.svelte'
  import OverlayDrawer from '$lib/components/layout/OverlayDrawer.svelte'
  import BookADemo from '$lib/components/other/BookADemo.svelte'
  import CreatePipelineButton from '$lib/components/pipelines/CreatePipelineButton.svelte'
  import { useInterval } from '$lib/compositions/common/useInterval.svelte'
  import { useClusterHealth } from '$lib/compositions/health/useClusterHealth.svelte'
  import { useAdaptiveDrawer } from '$lib/compositions/layout/useAdaptiveDrawer.svelte'
  import { useContextDrawer } from '$lib/compositions/layout/useContextDrawer.svelte'
  import { useGlobalDialog } from '$lib/compositions/layout/useGlobalDialog.svelte'
  import { useRefreshPipelineList } from '$lib/compositions/pipelines/usePipelineList.svelte'
  import { usePipelineAction } from '$lib/compositions/usePipelineAction.svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import { useSystemMessages } from '$lib/compositions/useSystemMessages.svelte'
  import { useToast } from '$lib/compositions/useToastNotification'
  import { getConfig } from '$lib/services/pipelineManager'
  import type { Snippet } from '$lib/types/svelte'
  import type { LayoutData } from './$types'

  const dialog = useGlobalDialog()

  const { children, data }: { children: Snippet; data: LayoutData } = $props()

  useRefreshPipelineList()
  usePipelineAction()

  const rightDrawer = useAdaptiveDrawer('right')
  const contextDrawer = useContextDrawer()

  const systemMessages = useSystemMessages()
  const clusterHealth = useClusterHealth()
  const now = useInterval(() => new Date(), 3600000, 3600000 - (Date.now() % 3600000))

  // Check for backend version changes every 30 seconds
  useInterval(
    async () => {
      try {
        const config = await getConfig()
        const currentVersion = data.feldera?.version
        const currentRevision = data.feldera?.revision
        if (
          currentVersion &&
          currentRevision &&
          (config.version !== currentVersion || config.revision !== currentRevision)
        ) {
          // Automatically refresh the page data to get the new backend version
          await invalidateAll()

          // Show a notification that the backend was updated
          const msgId = `backend_version_changed`

          const dismissTimeout = setTimeout(() => systemMessages.dismiss(msgId), 30000) // Auto-dismiss after 30 seconds
          systemMessages.upsert(msgId, {
            id: msgId,
            text: `Feldera was updated from version ${currentVersion} to ${config.version}.`,
            dismissable: { forMs: 0 },
            onDismiss: () => clearTimeout(dismissTimeout)
          })
        }
      } catch (e) {
        // Silently ignore errors when checking for version changes
        console.error('Failed to check backend version:', e)
      }
    },
    10000 // Check every 10 seconds
  )

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
  const healthMessage = $derived(
    clusterHealth.current.api !== 'healthy'
      ? 'There is an issue with the API server.'
      : clusterHealth.current.compiler !== 'healthy'
        ? 'There is an issue with the program compiler.'
        : clusterHealth.current.runner !== 'healthy'
          ? 'There is an issue with the Kubernetes runner.'
          : null
  )

  const api = usePipelineManager()
  const { toastMain, dismissMain } = useToast()
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
  color={'var(--color-primary-500)'}
  showSpinner={false}
  ignoreBeforeNavigate={() => false}
  ignoreAfterNavigate={() => false}
></SvelteKitTopLoader>
<div
  class="flex h-full w-full flex-col {api.isNetworkHealthy
    ? ''
    : 'disabled pointer-events-auto select-text [&_.monaco-editor-background]:pointer-events-none [&_[role="button"]]:pointer-events-none [&_[role="separator"]]:pointer-events-none [&_a]:pointer-events-none [&_button]:pointer-events-none'}"
  style={api.isNetworkHealthy ? '' : ''}
>
  {#if healthMessage && !page.url.pathname.startsWith('/health')}
    <LineBanner variant="error">
      {#snippet start()}
        <span>{healthMessage}</span>
        {@render BannerButton({
          text: 'See details',
          href: '/health/'
        })}
      {/snippet}
    </LineBanner>
  {/if}
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
  {@render children()}

  <OverlayDrawer
    width="w-72"
    bind:open={rightDrawer.value}
    side="right"
    modal={true}
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
    <BookADemo class="btn self-center preset-filled-primary-500">Book a demo</BookADemo>
    <NavigationExtras inline></NavigationExtras>
  </OverlayDrawer>
  <OverlayDrawer
    width="w-[100vw] md:w-[50vw] max-w-3xl"
    side="right"
    bind:open={() => !!contextDrawer.content, () => (contextDrawer.content = null)}
    modal={false}
    class="bg-white-dark scrollbar overflow-auto p-4 pb-0 md:p-6 md:pb-0"
  >
    {@render contextDrawer.content?.()}
  </OverlayDrawer>
</div>
<GlobalModal
  dialog={dialog.dialog}
  onClose={() => {
    dialog.onclose?.()
    dialog.dialog = null
  }}
></GlobalModal>
