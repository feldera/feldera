<script lang="ts">
  import '../app.css'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  import { BodyAttr } from 'svelte-attr'
  import '@fortawesome/fontawesome-free/css/brands.min.css'

  import posthog from 'posthog-js'
  import { browser } from '$app/environment'
  import { beforeNavigate, afterNavigate } from '$app/navigation'
  import { Toaster } from 'svelte-french-toast'

  import '$assets/fonts/feldera-material-icons.css'
  import '$assets/fonts/generic-icons.css'

  import { useInterval } from '$lib/compositions/common/useInterval.svelte'
  import { getLicenseMessage } from '$lib/functions/license'
  import { newDate } from '$lib/compositions/serverTime'
  import { page } from '$app/state'
  import { useSystemMessages } from '$lib/compositions/useSystemMessages.svelte'

  let { children } = $props()
  let darkMode = useDarkMode()

  if (browser) {
    beforeNavigate(() => posthog.capture('$pageleave'))
    afterNavigate(() => posthog.capture('$pageview'))
  }

  const { upsert } = useSystemMessages()
  useInterval(() => {
    if (!page.data.feldera) {
      return
    }
    upsert(/^license_/, getLicenseMessage(page.data.feldera.config, newDate()))
  }, 60000)
</script>

<BodyAttr
  class="{darkMode.current} scrollbar-thumb-surface-200 scrollbar-thumb-rounded-full scrollbar-w-2.5 scrollbar-h-2.5 hover:scrollbar-thumb-surface-400 dark:scrollbar-thumb-surface-800 dark:hover:scrollbar-thumb-surface-600"
/>

<Toaster position={'bottom-right'} toastOptions={{}}></Toaster>
{@render children?.()}

<style lang="scss" global>
  .toast-error {
    @apply bottom-8 top-auto bg-error-50-950;
  }
</style>
