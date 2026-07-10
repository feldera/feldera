<script lang="ts">
  interface Props {
    /** Pipeline runtime configuration, as parsed from `pipeline_config.json`. */
    runtimeConfig: unknown
  }

  let { runtimeConfig }: Props = $props()

  // Pretty-print the config for a read-only view.
  const formatted = $derived(
    runtimeConfig === undefined ? '' : JSON.stringify(runtimeConfig, null, 2)
  )
</script>

<div class="absolute inset-0 overflow-auto scrollbar bg-white-dark rounded">
  {#if formatted === ''}
    <div class="flex h-full items-center justify-center font-mono text-sm text-surface-600-400">
      No runtime configuration available in this bundle
    </div>
  {:else}
    <pre class="p-4 font-mono text-sm text-surface-900-100">{formatted}</pre>
  {/if}
</div>
