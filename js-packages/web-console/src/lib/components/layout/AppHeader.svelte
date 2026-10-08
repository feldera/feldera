<script lang="ts">
  import FelderaModernLogoColorDark from '$assets/images/feldera-modern/Feldera Logo Color Dark.svg?component'
  import FelderaModernLogoColorLight from '$assets/images/feldera-modern/Feldera Logo Color Light.svg?component'
  import FelderaModernLogomarkColorDark from '$assets/images/feldera-modern/Feldera Logomark Color Dark.svg?component'
  import FelderaModernLogomarkColorLight from '$assets/images/feldera-modern/Feldera Logomark Color Light.svg?component'
  import ProfileButton from '$lib/components/auth/ProfileButton.svelte'
  import { useClusterHealth } from '$lib/compositions/health/useClusterHealth.svelte'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  import { resolve } from '$lib/functions/svelte'
  import type { Snippet } from '$lib/types/svelte'

  const { afterStart, beforeEnd }: { afterStart?: Snippet; beforeEnd?: Snippet } = $props()
  const darkMode = useDarkMode()

  const healthStatus = useClusterHealth()
</script>

<div class="flex flex-row items-center justify-between gap-2 px-2 py-1.5 md:px-8">
  <a class="flex h-12 items-center lg:items-start lg:pr-2.5" href={resolve('/')}>
    <span class="hidden lg:flex">
      {#if darkMode.current === 'dark'}
        <FelderaModernLogoColorLight class="h-[30px]"></FelderaModernLogoColorLight>
      {:else}
        <FelderaModernLogoColorDark class="h-[30px]"></FelderaModernLogoColorDark>
      {/if}
    </span>
    <span class="flex lg:hidden">
      {#if darkMode.current === 'dark'}
        <FelderaModernLogomarkColorLight class="h-8"></FelderaModernLogomarkColorLight>
      {:else}
        <FelderaModernLogomarkColorDark class="h-8"></FelderaModernLogomarkColorDark>
      {/if}
    </span>
  </a>
  {@render afterStart?.()}
  <!-- <div class="flex flex-1"></div> -->
  <div class="-mr-2 ml-auto"></div>
  {@render beforeEnd?.()}
  <ProfileButton compactBreakpoint="xl:" healthStatus={healthStatus.current}></ProfileButton>
</div>
