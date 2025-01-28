<script lang="ts">
  import { page } from '$app/state'
  import { useLatestVersion } from '$lib/compositions/latestVersion.svelte'

  const { latestVersion } = useLatestVersion(fetch)
</script>

<div class="hr flex justify-between pt-4 text-surface-500">
  <span>Feldera {page.data.feldera.version} {page.data.feldera.edition}</span>
  {#if latestVersion}
    <span>Latest: {latestVersion}</span>
  {/if}
</div>
