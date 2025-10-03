<script lang="ts">
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  import { page } from '$app/state'

  import FelderaLogoBlackDetail from '$assets/images/feldera-modern/Feldera Logo Black Detail.svg?component'
  import FelderaLogoWhiteDetail from '$assets/images/feldera-modern/Feldera Logo White Detail.svg?component'
  import BookADemo from '../other/BookADemo.svelte'

  const darkMode = useDarkMode()
</script>

<div class="flex w-full flex-row justify-between whitespace-nowrap px-2 py-4 sm:px-5">
  {#if darkMode.current === 'dark'}
    <FelderaLogoWhiteDetail class="h-8 pl-4 opacity-40"></FelderaLogoWhiteDetail>
  {:else}
    <FelderaLogoBlackDetail class="h-8 pl-4 opacity-40"></FelderaLogoBlackDetail>
  {/if}
  <div class="flex flex-col gap-1 md:flex-row">
    <BookADemo class="justify-start px-4 hover:bg-surface-50-950">
      <!-- svelte-ignore block_empty -->
      {#snippet icon()}{/snippet}
      Book a demo
    </BookADemo>

    {#if page.data.feldera}
      <a
        target="_blank"
        rel="noreferrer"
        href={page.data.feldera.changelog}
        class="btn justify-start px-4 hover:bg-surface-50-950"
        >{page.data.feldera.edition} {page.data.feldera.version} Changelog</a
      >
    {/if}
    <div class="btn justify-start px-4">© Feldera {new Date().getUTCFullYear()}</div>
  </div>
</div>
