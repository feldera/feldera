<!-- Render text decorated by ANSI escape sequences -->
<script lang="ts" module>
  import { FancyAnsi } from 'fancy-ansi'
  const fancyAnsi = new FancyAnsi()
</script>

<script lang="ts">
  let { value }: { value: string } = $props()
</script>

{#if value}
  {@html fancyAnsi.toHtml(value)}
{/if}
