<script lang="ts">
  import '../app.css'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  import { BodyAttr } from 'svelte-attr'
  import '@fortawesome/fontawesome-free/css/brands.min.css'

  import posthog from 'posthog-js'
  import { browser } from '$app/environment'
  import { beforeNavigate, afterNavigate } from '$app/navigation'
  import { ToastProvider } from '@skeletonlabs/skeleton-svelte'

  // import type { Action } from 'svelte/action'
  import 'virtual:feldera-material-icons-webfont.css'
  import 'virtual:generic-icons-webfont.css'

  // export const classList: Action<Element, string | string[]> = (node, classes) => {
  //   const tokens = Array.isArray(classes) ? classes : [classes]
  //   node.classList.add(...tokens)

  //   return {
  //     destroy() {
  //       node.classList.remove(...tokens)
  //     }
  //   }
  // }
  let { children } = $props()
  let darkMode = useDarkMode()

  if (browser) {
    beforeNavigate(() => posthog.capture('$pageleave'))
    afterNavigate(() => posthog.capture('$pageview'))
  }
</script>

<BodyAttr
  class="{darkMode.current} scrollbar-thumb-surface-200 scrollbar-thumb-rounded-full scrollbar-w-2.5 scrollbar-h-2.5 hover:scrollbar-thumb-surface-400 dark:scrollbar-thumb-surface-800 dark:hover:scrollbar-thumb-surface-600"
/>

<ToastProvider
  groupClasses="pointer-events-none"
  toastClasses="pointer-events-auto"
  messageClasses="!text-base max-w-sm"
  btnDismissClasses="!text-xl"
>
  {@render children()}
</ToastProvider>
