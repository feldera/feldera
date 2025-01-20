<script lang="ts">
  import '../app.css'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  import { BodyAttr } from 'svelte-attr'
  import '@fortawesome/fontawesome-free/css/brands.min.css'
  import dmSans from '@fontsource-variable/dm-sans/files/dm-sans-latin-wght-normal.woff2'
  import dmSansExt from '@fontsource-variable/dm-sans/files/dm-sans-latin-ext-wght-normal.woff2'

  import posthog from 'posthog-js'
  import { browser } from '$app/environment'
  import { beforeNavigate, afterNavigate } from '$app/navigation'

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

<svelte:head>
  <!-- TODO: Check if preloading works -->
  <link rel="preload" href={dmSans} as="font" type="font/woff2" crossorigin="anonymous" />
  <link rel="preload" href={dmSansExt} as="font" type="font/woff2" crossorigin="anonymous" />
</svelte:head>

<BodyAttr
  class="{darkMode.current} scrollbar-thumb-surface-200 scrollbar-thumb-rounded-full scrollbar-w-2.5 scrollbar-h-2.5 hover:scrollbar-thumb-surface-400 dark:scrollbar-thumb-surface-800 dark:hover:scrollbar-thumb-surface-600"
/>

{@render children()}
