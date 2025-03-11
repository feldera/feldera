<script lang="ts">
  import { clipboard } from '@svelte-bin/clipboard'
  import { clickedClass } from '$lib/compositions/actions/clickedClass'

  let { value, class: _class }: { value: string; class?: string } = $props()
</script>

<button
  class="btn-icon flex-none {_class}"
  use:clipboard={value}
  use:clickedClass={{
    base: 'fd fd-copy',
    clicked: 'fd fd-check text-success-500 text-[20px] pointer-events-none'
  }}
  aria-label="Copy to clipboard"
></button>
