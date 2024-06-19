<script lang="ts">
  import '../app.css'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  import { BodyAttr } from 'svelte-attr'
  import '@fortawesome/fontawesome-free/css/brands.min.css'

  import { createClient } from '@hey-api/client-fetch'
  import JSONbig from 'true-json-bigint'
  import type { Action } from 'svelte/action'
  import { felderaEndpoint } from '$lib/functions/configs/felderaEndpoint'
  createClient({
    bodySerializer: JSONbig.stringify,
    responseTransformer: JSONbig.parse as any,
    baseUrl: felderaEndpoint
  })

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
</script>

<BodyAttr class={darkMode.value} />

{@render children()}
