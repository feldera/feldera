<script lang="ts">
  import { clipboard } from '@svelte-bin/clipboard'
  import JSONbig from 'true-json-bigint'
  import type { SystemError } from '$lib/compositions/health/systemErrors'
  import type { Snippet } from '$lib/types/svelte'

  const { systemError, before }: { systemError: SystemError; before?: Snippet } = $props()
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
    class="m-0 max-h-48 overflow-x-auto bg-surface-50-950 p-2 pt-6 font-mono text-sm whitespace-pre"
  >
    {text}
  </div>
  <button
    class="absolute top-2 right-4 btn-icon preset-tonal-surface text-[20px]"
    use:clipboard={text}
  >
    <div class="fd fd-copy"></div>
  </button>
</div>
