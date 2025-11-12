<script lang="ts">
  import type { SystemError } from '$lib/compositions/health/systemErrors'
  import JSONbig from 'true-json-bigint'
  import { clipboard } from '@svelte-bin/clipboard'
  import type { Snippet } from '$lib/types/svelte'

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
    class="m-0 max-h-48 overflow-x-auto whitespace-pre p-2 pt-6 font-mono text-sm bg-surface-50-950"
  >
    {text}
  </div>
  <button
    class="btn-icon absolute right-4 top-2 text-[20px] preset-tonal-surface"
    use:clipboard={text}
  >
    <div class="fd fd-copy"></div>
  </button>
</div>
