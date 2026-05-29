<script lang="ts" module>
  import type { SourcePositionRange } from 'profiler-lib'
  import type { Snippet } from 'svelte'

  export type SqlTabProps = {
    sqlPanel: Snippet<[SourcePositionRange[]]> | undefined
    code: string
    highlightRanges: SourcePositionRange[]
  }
</script>

<script lang="ts">
  import SqlCodeView from '../SqlCodeView.svelte'
  let { sqlPanel, code, highlightRanges }: SqlTabProps = $props()
</script>

<div class="relative min-h-0 flex-1 bg-white-dark">
  <div class="absolute inset-0 overflow-auto">
    {#if sqlPanel}
      {@render sqlPanel(highlightRanges)}
    {:else}
      <SqlCodeView {code} {highlightRanges} />
    {/if}
  </div>
</div>
