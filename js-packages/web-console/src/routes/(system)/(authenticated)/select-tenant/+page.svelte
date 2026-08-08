<script lang="ts">
  import TenantPicker from '$lib/components/auth/TenantPicker.svelte'
  import AppHeader from '$lib/components/layout/AppHeader.svelte'
  import type { PageData } from './$types'

  const { data }: { data: PageData } = $props()
</script>

<!-- Only the header and the picker: no acting tenant means no pipelines, health
     or banners to show, and the header's tenant-scoped items hide themselves
     when the session reports no tenant, leaving the logo, the theme switch and
     sign-out. -->
<div class="flex h-full flex-col">
  <AppHeader></AppHeader>
  <TenantPicker memberships={data.memberships}></TenantPicker>
</div>
