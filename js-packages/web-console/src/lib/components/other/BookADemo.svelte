<script lang="ts">
  import { useConceptualHq } from '$lib/compositions/useConceptualHq.svelte'
  import { captureEvent } from '$lib/services/analytics'
  import { bookADemoUrl } from '$lib/services/calendly'
  import type { Snippet } from '$lib/types/svelte'

  const {
    class: _class,
    children,
    icon = defaultIcon,
    placement
  }: {
    class?: string
    children?: Snippet
    icon?: Snippet
    /** Where this button sits, e.g. 'footer'. Reaches Calendly and analytics. */
    placement?: string
  } = $props()

  const conceptualHq = useConceptualHq()
  const calendlyUrl = $derived(bookADemoUrl({ visitorId: conceptualHq.deviceId, placement }))
</script>

{#snippet defaultIcon()}
  <span class="fd fd-rocket text-[20px]"></span>
{/snippet}

<a
  class={_class}
  href={calendlyUrl}
  target="_blank"
  rel="noreferrer"
  onclick={() => captureEvent('calendly_opened', { url: calendlyUrl, placement })}
>
  {@render icon()}
  {@render children?.()}
</a>
