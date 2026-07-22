<script lang="ts">
  import { captureEvent } from '$lib/services/analytics'
  import type { Snippet } from '$lib/types/svelte'

  const {
    class: _class,
    children,
    icon = defaultIcon,
    triggerLocation
  }: { class?: string; children?: Snippet; icon?: Snippet; triggerLocation?: string } = $props()

  const calendlyUrl = 'https://calendly.com/d/cqnj-p63-mbq/feldera-demo'
</script>

{#snippet defaultIcon()}
  <span class="fd fd-rocket text-[20px]"></span>
{/snippet}

<a
  class={_class}
  href={calendlyUrl}
  target="_blank"
  rel="noreferrer"
  onclick={() =>
    captureEvent('calendly_opened', { url: calendlyUrl, trigger_location: triggerLocation })}
>
  {@render icon()}
  {@render children?.()}
</a>
