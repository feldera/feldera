<script lang="ts">
  import type { Snippet } from '$lib/types/svelte'

  let {
    class: _class,
    children,
    icon = defaultIcon
  }: { class?: string; children?: Snippet; icon?: Snippet } = $props()
</script>

{#snippet defaultIcon()}
  <span class="fd fd-rocket text-[20px]"></span>
{/snippet}

<a
  class="btn {_class}"
  href="https://calendly.com/d/cqnj-p63-mbq/feldera-demo"
  target="_blank"
  rel="noreferrer"
>
  {@render icon()}
  {@render children?.()}
</a>
