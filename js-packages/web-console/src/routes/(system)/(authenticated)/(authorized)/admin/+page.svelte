<script lang="ts">
  import AdminPage from '$lib/components/admin/AdminPage.svelte'
  import AppHeader from '$lib/components/layout/AppHeader.svelte'
  import PipelineBreadcrumbs from '$lib/components/layout/PipelineBreadcrumbs.svelte'
  import { resolve } from '$lib/functions/svelte'
</script>

<svelte:head>
  <title>Administration — Feldera</title>
</svelte:head>

<div class="flex h-screen flex-col overflow-hidden">
  <AppHeader>
    {#snippet afterStart()}
      <PipelineBreadcrumbs
        breadcrumbs={[
          { text: 'Home', href: resolve('/') },
          { text: 'Admin', href: resolve('/admin') }
        ]}
      ></PipelineBreadcrumbs>
    {/snippet}
  </AppHeader>

  <div class="scrollbar flex-1 overflow-auto pt-4">
    <AdminPage></AdminPage>
  </div>
</div>
