<script lang="ts">
  import { usePipelineList } from '#lib/compositions/pipelines/usePipelineList.svelte.ts'
  import { useTryPipeline } from '#lib/compositions/pipelines/useTryPipeline.ts'
  import { usePermission } from '#lib/compositions/usePermission.svelte.ts'
  import type { Demo } from '#lib/services/pipelineManager.ts'

  let { demo, placement }: { demo: Demo | null; placement?: string } = $props()
  const tryPipeline = useTryPipeline()
  const canCreate = usePermission('write:pipeline')
  const list = usePipelineList()
  // A read-only caller may open a demo only when its pipeline already exists
  // (the tile then navigates to it); creating one from a demo needs write access.
  const exists = $derived(!!demo && !!list.pipelines?.some((p) => p.name === demo.name))
  const enabled = $derived(exists || canCreate.allowed)

  // Icon class for each demo, keyed by demo name
  const demoIconClasses: Record<string, string> = {
    'accelerating-batch-analytics': 'fd-zap',
    'ecommerce-medallion-architecture': 'fd-layers',
    'fraud-detection': 'fd-shield-alert',
    'sec-ops': 'fd-radar',
    'product-availability': 'fd-package-check',
    'feature-engineering': 'fd-brain-circuit',
    'supply-chain': 'fd-boxes',
    'udf-tutorial': 'fd-code-xml',
    'time-series-tutorial': 'fd-chart-line',
    'fine-grained-auth': 'fd-key-round',
    'dynamic-fga': 'fd-shield-check',
    spreadsheet: 'fd-table-2'
  }
</script>

{#if demo}
  <button
    class="flex flex-col card rounded-[8px] border border-line bg-page p-4 text-left transition-colors hover:bg-card-hover disabled:pointer-events-none disabled:opacity-50"
    disabled={!enabled}
    onclick={() => tryPipeline(demo, placement)}
  >
    <div
      class="flex h-9 w-9 shrink-0 items-center justify-center rounded-md bg-accent-subtle text-accent"
    >
      <span class="fd {demoIconClasses[demo.name] ?? 'fd-workflow'} text-[18px]" aria-hidden="true"
      ></span>
    </div>
    <div class="pt-3 text-sm text-surface-700-300">{demo.type}</div>
    <span class="py-2 font-semibold">{demo.title}</span>
    <span class="text-surface-700-300">{demo.description}</span>
  </button>
{:else}
  <div class="flex flex-col card rounded-[8px] border border-line bg-page p-4">
    <div class="flex flex-col gap-1">
      <div class="placeholder w-16 animate-pulse"></div>
      <span class="placeholder w-48 animate-pulse bg-surface-700-300"></span>
      <span class="placeholder animate-pulse"></span>
      <span class="placeholder animate-pulse"></span>
      <span class="placeholder w-1/2 animate-pulse"></span>
    </div>
    <span class="text-left text-surface-700-300"></span>
  </div>
{/if}
