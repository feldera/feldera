<script lang="ts">
  import { page } from '$app/state'
  import AppHeader from '$lib/components/layout/AppHeader.svelte'
</script>

<AppHeader></AppHeader>

<div class="pt:16 flex flex-col gap-4 p-8 text-center sm:pt-32">
  <div class="h3 sm:h1">Unable to reach the Feldera service</div>
  <div class="h4 sm:h3">Please make sure the Feldera service is running and reload the page.</div>
  <div class="h5">Error message: {page.error?.message}</div>
</div>
