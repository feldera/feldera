<script lang="ts">
  import FelderaModernLogoBlack from '#assets/images/feldera-modern/Feldera Logo Black Solid.svg?component'
  import FelderaModernLogoWhite from '#assets/images/feldera-modern/Feldera Logo White Solid.svg?component'
  import FelderaModernLogomarkBlack from '#assets/images/feldera-modern/Feldera Logomark Black.svg?component'
  import FelderaModernLogomarkWhite from '#assets/images/feldera-modern/Feldera Logomark White.svg?component'
  import ProfileButton from '#lib/components/auth/ProfileButton.svelte'
  import { useClusterHealth } from '#lib/compositions/health/useClusterHealth.svelte.ts'
  import { useDarkMode } from '#lib/compositions/useDarkMode.svelte.ts'
  import { resolve } from '#lib/functions/svelte.ts'
  import type { Snippet } from '#lib/types/svelte.ts'

  const {
    afterStart,
    beforeEnd,
    class: class_ = 'px-2 md:px-8'
  }: {
    afterStart?: Snippet
    beforeEnd?: Snippet
    class?: string
  } = $props()
  const darkMode = useDarkMode()

  const healthStatus = useClusterHealth()
</script>

<div class="flex flex-row items-center justify-between gap-2 border-b border-line py-1 {class_}">
  <a class="flex h-12 items-center lg:items-start lg:pr-2.5" href={resolve('/')}>
    <span class="hidden lg:flex">
      {#if darkMode.current === 'dark'}
        <FelderaModernLogoWhite class="h-[30px]"></FelderaModernLogoWhite>
      {:else}
        <FelderaModernLogoBlack class="h-[30px] fill-[#050505]"></FelderaModernLogoBlack>
      {/if}
    </span>
    <span class="flex lg:hidden">
      {#if darkMode.current === 'dark'}
        <FelderaModernLogomarkWhite class="h-8"></FelderaModernLogomarkWhite>
      {:else}
        <FelderaModernLogomarkBlack class="h-8 fill-[#050505]"></FelderaModernLogomarkBlack>
      {/if}
    </span>
  </a>
  {@render afterStart?.()}
  <!-- <div class="flex flex-1"></div> -->
  <div class="-mr-2 ml-auto"></div>
  {@render beforeEnd?.()}
  <ProfileButton compactBreakpoint="xl:" healthStatus={healthStatus.current}></ProfileButton>
</div>
