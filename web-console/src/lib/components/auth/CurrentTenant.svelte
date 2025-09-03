<script lang="ts">
  import { page } from '$app/state'
</script>

{#if page.data.feldera}
  <div class="text-nowrap text-end">
    <span class="pr-2">• Tenant</span>
    <span class="font-mono">{page.data.feldera.tenantName}</span>
  </div>
{/if}
