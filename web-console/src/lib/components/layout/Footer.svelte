<script lang="ts">
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  import { page } from '$app/state'

  import FelderaLogoBlackDetail from '$assets/images/feldera-modern/Feldera Logo Black Detail.svg?component'
  import FelderaLogoWhiteDetail from '$assets/images/feldera-modern/Feldera Logo White Detail.svg?component'

  const darkMode = useDarkMode()
</script>

<div class="flex w-full flex-row justify-between whitespace-nowrap px-2 py-4 sm:px-5">
  {#if darkMode.current === 'dark'}
    <FelderaLogoWhiteDetail class="h-8 pl-4 opacity-40"></FelderaLogoWhiteDetail>
  {:else}
    <FelderaLogoBlackDetail class="h-8 pl-4 opacity-40"></FelderaLogoBlackDetail>
  {/if}
  <div class="flex flex-col gap-1 md:flex-row">
    <a
      href="https://calendly.com/d/cqnj-p63-mbq/feldera-demo"
      target="_blank"
      rel="noreferrer"
      class="btn justify-start px-4 hover:bg-surface-50-950">Book a demo</a
    >
    <a
      target="_blank"
      rel="noreferrer"
      href="https://github.com/feldera/feldera/releases/tag/v{page.data.felderaVersion}"
      class="btn justify-start px-4 hover:bg-surface-50-950"
      >v{page.data.felderaVersion} Changelog</a
    >
    <div class="btn justify-start px-4">© Feldera {new Date().getUTCFullYear()}</div>
  </div>
</div>
