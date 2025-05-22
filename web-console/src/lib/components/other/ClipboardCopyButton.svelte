<script lang="ts">
  import { clipboard } from '@svelte-bin/clipboard'
  import { clickedClass } from '$lib/compositions/actions/clickedClass'
  import type { Snippet } from 'svelte'

  let {
    value,
    class: _class,
    children
  }: { value: string; class?: string; children?: Snippet } = $props()
</script>

<button
  class="btn-icon flex-none before:w-4 {_class}"
  use:clipboard={value}
  use:clickedClass={{
    base: 'fd fd-copy',
    clicked: 'fd fd-check text-success-500 text-[20px] pointer-events-none'
  }}
  aria-label="Copy to clipboard"
>
  {@render children?.()}</button
>
