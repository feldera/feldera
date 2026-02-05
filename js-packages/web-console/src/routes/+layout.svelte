<script lang="ts">
  import './layout.css'
  import { BodyAttr, HtmlAttr } from 'svelte-attr'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  import '@fortawesome/fontawesome-free/css/brands.min.css'

  import posthog from 'posthog-js'
  import { Toaster } from 'svelte-french-toast'
  import { browser } from '$app/environment'
  import { afterNavigate, beforeNavigate } from '$app/navigation'

  import '$assets/fonts/feldera-material-icons.css'
  import '$assets/fonts/generic-icons.css'

  import { page } from '$app/state'
  import { useInterval } from '$lib/compositions/common/useInterval.svelte'
  import { newDate } from '$lib/compositions/serverTime'
  import { useSystemMessages } from '$lib/compositions/useSystemMessages.svelte'
  import { getLicenseMessage } from '$lib/functions/license'

  const { children } = $props()
  const darkMode = useDarkMode()

  if (browser) {
    beforeNavigate(() => posthog.capture('$pageleave'))
    afterNavigate(() => posthog.capture('$pageview'))
  }

  // Scarf.sh tracking for OSS deployments
  // Only track Open source edition (excludes Enterprise builds)
  const shouldTrack = $derived(browser && page.data.feldera?.config?.edition === 'Open source')
  const scarfPixelId = 'c5e8a21a-5f5a-424d-81c4-5ded97174900'
  const scarfTrackingUrl = $derived(
    shouldTrack
      ? `https://static.scarf.sh/a.png?x-pxid=${scarfPixelId}&version=${encodeURIComponent(page.data.feldera!.config!.version)}&build_source=${encodeURIComponent(page.data.feldera!.config!.build_source)}`
      : ''
  )

  const { upsert } = useSystemMessages()
  useInterval(() => {
    if (!page.data.feldera) {
      return
    }
    upsert(/^license_/, getLicenseMessage(page.data.feldera.config, newDate()))
  }, 60000)
</script>

<HtmlAttr class={darkMode.current}></HtmlAttr>

<BodyAttr
  class="text-base scrollbar-h-[10px] scrollbar-thumb-primary-200 scrollbar-thumb-rounded-full scrollbar-w-[10px] dark:scrollbar-thumb-surface-600 scrollbar-hover:scrollbar-thumb-surface-400 dark:scrollbar-hover:scrollbar-thumb-surface-600"
/>

<Toaster position={'bottom-right'} toastOptions={{}}></Toaster>
{@render children?.()}

{#if shouldTrack}
  <img src={scarfTrackingUrl} alt="" aria-hidden="true" style="display: none;" />
{/if}

<style lang="scss" global>
  .toast-error {
    @apply top-auto bottom-8 bg-error-50-950;
  }
</style>
