<script lang="ts">
  import { page } from '$app/state'
</script>

<div class="hr flex justify-between pt-4 text-surface-500">
  <span>Feldera {page.data.feldera.version} {page.data.feldera.edition}</span>
  {#if page.data.feldera.update?.version}
    <span>latest: {page.data.feldera.update.version}</span>
  {/if}
</div>
