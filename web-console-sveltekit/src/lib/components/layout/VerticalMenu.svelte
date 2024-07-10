<script lang="ts">
  import { verticalNavItems } from '$lib/functions/navigation/vertical'
  import type { NavLink, NavSectionTitle } from '$lib/types/layout'
  import FelderaLogo from '$assets/images/feldera/LogoSolid.svg?component'
  import { page } from '$app/stores'
  const isNavLinkActive = (item: { path?: string | string[] }) =>
    (Array.isArray(item.path) ? item.path : [item.path]).find(
      (path) => path && $page.url.pathname.startsWith(path) && path !== '/'
    )
</script>

<div class="flex w-56 flex-col gap-1 pr-4">
  <a href="/">
    <FelderaLogo class="w-40 p-3"></FelderaLogo>
  </a>
  {#each verticalNavItems({ showSettings: false }) as item}
    {#if 'sectionTitle' in item}
      <div
        class="text-surface-500 before:border-surface-500 after:border-surface-500 flex items-center py-2 text-sm uppercase before:me-6 before:flex-1 before:border-t after:ms-6 after:flex-1 after:border-t">
        {item.sectionTitle}
      </div>
    {:else}
      <a
        href={Array.isArray(item.path) ? item.path[0] : item.path}
        class={'flex h-8 flex-nowrap items-center gap-4 rounded-r-full px-6 py-1 ' +
          (isNavLinkActive(item)
            ? 'bg-primary-500 text-surface-contrast-600'
            : 'text-surface-800-200 hover:bg-surface-100/50')}>
        <div class={item.class + ' text-[24px] '}></div>
        <span>{item.title}</span>
      </a>{/if}{/each}
</div>
