<script lang="ts">
  import type { SystemError } from '$lib/compositions/health/systemErrors'
  import JSONbig from 'true-json-bigint'
  import { clipboard } from '@svelte-bin/clipboard'
  import type { Snippet } from 'svelte'

  let { systemError, before }: { systemError: SystemError; before?: Snippet } = $props()
  const text = $derived(
    JSONbig.stringify(systemError.cause.body, undefined, '\t')
      .replaceAll('\\n', '\n\t')
      .replaceAll('\\"', '"')
  )
</script>

<div class="text-sm">
  {@render before?.()}
  {systemError.message}
</div>
<div class="relative">
  <div
    class="bg-surface-50-950 m-0 max-h-48 overflow-x-auto whitespace-pre p-2 pt-6 font-mono text-sm">
    {text}
  </div>
  <button
    class="btn-icon preset-tonal-surface absolute right-4 top-2 text-[20px]"
    use:clipboard={text}>
    <div class="bx bx-copy"></div>
  </button>
</div>
