<script lang="ts">
  let {
    breadcrumbs
  }: {
    breadcrumbs: { text: string; href?: string; onclick?: () => void }[]
  } = $props()
</script>

<div class="overflow-hidden overflow-ellipsis whitespace-nowrap">
  {#each breadcrumbs as breadcrumb}
    <a
      class="h6 inline cursor-pointer self-center font-medium sm:h5 sm:inline sm:font-medium [&:not(:nth-last-child(2))]:text-surface-500"
      href={breadcrumb.href}
      onclick={breadcrumb.onclick}>{breadcrumb.text}</a
    >
    <span
      class="h6 inline self-center px-2 font-medium !text-surface-500 sm:h5 last:hidden sm:inline"
      >/</span
    >
  {/each}
</div>
