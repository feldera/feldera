<script lang="ts">
  import { clipboard, type Parameter } from '@svelte-bin/clipboard'
  import { clickedClass } from '$lib/compositions/actions/clickedClass'
  import type { Snippet } from '$lib/types/svelte'

  const {
    value,
    class: _class,
    children
  }: { value: Parameter; class?: string; children?: Snippet } = $props()
</script>

<button
  class="btn-icon flex h-9 flex-none before:w-4 before:transition-transform {_class}"
  use:clipboard={value}
  use:clickedClass={{
    base: 'fd fd-copy',
    clicked: 'fd fd-check text-success-500 text-[20px] pointer-events-none before:-translate-x-0.5'
  }}
  aria-label="Copy to clipboard"
>
  {@render children?.()}</button
>
