<script lang="ts">
  import { useTryPipeline } from '$lib/compositions/pipelines/useTryPipeline'
  import type { Demo } from '$lib/services/manager'

  const { demo }: { demo: Demo & { type: string } } = $props()
  const tryPipeline = useTryPipeline()
</script>

<div class="flex flex-col card border border-surface-100-900 p-4">
  <div class="text-sm text-surface-700-300">{demo.type}</div>
  <button class="text-left" onclick={() => tryPipeline(demo)}>
    <span class="py-2 font-semibold">{demo.title}</span>
    <!-- <span class="fd fd-arrow-right inline-block w-2 text-[20px]"></span> -->
  </button>
  <span class="text-left text-surface-700-300">{demo.description}</span>
</div>
