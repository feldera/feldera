<script lang="ts">
  import { page } from '$app/state'
  import AppHeader from '$lib/components/layout/AppHeader.svelte'
</script>

<div class="flex h-full w-full max-w-[3200px] flex-col place-self-center">
  <AppHeader></AppHeader>

  <div class="pt:16 p-8 text-center sm:pt-32">
    <div
      class="h1 inline-block bg-gradient-to-br from-fuchsia-300 via-amber-50 to-orange-300 bg-clip-text text-[160px] text-transparent sm:text-[300px] dark:from-fuchsia-700 dark:via-amber-950 dark:to-orange-700"
    >
      {page.status}
    </div>
    <div class="h3">
      {page.error?.message === 'Not Found'
        ? 'This page is not a part of your Feldera instance'
        : page.error?.message}
    </div>
  </div>
</div>
