<script lang="ts">
  import '../app.css'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  import { BodyAttr } from 'svelte-attr'
  import '@fortawesome/fontawesome-free/css/brands.min.css'

  import posthog from 'posthog-js'
  import { browser } from '$app/environment'
  import { beforeNavigate, afterNavigate } from '$app/navigation'

  import type { Action } from 'svelte/action'
  import 'virtual:vite-svg-2-webfont.css'

  export const classList: Action<Element, string | string[]> = (node, classes) => {
    const tokens = Array.isArray(classes) ? classes : [classes]
    node.classList.add(...tokens)

    return {
      destroy() {
        node.classList.remove(...tokens)
      }
    }
  }
  let { children } = $props()
  let { darkMode } = useDarkMode()

  if (browser) {
    beforeNavigate(() => posthog.capture('$pageleave'))
    afterNavigate(() => posthog.capture('$pageview'))
  }
</script>

<BodyAttr class={darkMode.value} />

{@render children()}
