<script lang="ts">
  import FelderaModernLogoColorDark from '$assets/images/feldera-modern/Feldera Logo Color Dark.svg?component'
  import FelderaModernLogoColorLight from '$assets/images/feldera-modern/Feldera Logo Color Light.svg?component'
  import FelderaModernLogomarkColorDark from '$assets/images/feldera-modern/Feldera Logomark Color Dark.svg?component'
  import FelderaModernLogomarkColorLight from '$assets/images/feldera-modern/Feldera Logomark Color Light.svg?component'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  import type { Snippet } from 'svelte'
  import AuthButton from '$lib/components/auth/AuthButton.svelte'
  import { useRefreshPipelineList } from '$lib/compositions/pipelines/usePipelineList.svelte'
  import { base } from '$app/paths'

  let { afterStart, beforeEnd }: { afterStart?: Snippet; beforeEnd?: Snippet } = $props()
  let darkMode = useDarkMode()

  useRefreshPipelineList()
</script>

<div class="flex w-full flex-row items-center justify-between gap-4 px-2 py-2 md:px-8">
  <a class="py-1 md:pt-0 lg:pb-6 lg:pr-6" href="{base}/">
    <span class="hidden lg:block">
      {#if darkMode.current === 'dark'}
        <FelderaModernLogoColorLight class="h-12"></FelderaModernLogoColorLight>
      {:else}
        <FelderaModernLogoColorDark class="h-12"></FelderaModernLogoColorDark>
      {/if}
    </span>
    <span class="inline lg:hidden">
      {#if darkMode.current === 'dark'}
        <FelderaModernLogomarkColorLight class="h-8"></FelderaModernLogomarkColorLight>
      {:else}
        <FelderaModernLogomarkColorDark class="h-8"></FelderaModernLogomarkColorDark>
      {/if}
    </span>
  </a>
  {@render afterStart?.()}
  <!-- <div class="flex flex-1"></div> -->
  <div class="-mr-4 ml-auto"></div>
  {@render beforeEnd?.()}
  <AuthButton compactBreakpoint="xl:"></AuthButton>
</div>
