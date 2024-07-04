<script lang="ts">
  import HealthPopup from '$lib/components/health/HealthPopup.svelte'
  import Drawer from '$lib/components/layout/Drawer.svelte'
  import GlobalModal from '$lib/components/layout/GlobalModal.svelte'
  import VerticalMenu from '$lib/components/layout/VerticalMenu.svelte'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  import type { Snippet } from 'svelte'
  let { children } = $props<{ children: Snippet }>()
  let { darkMode, toggleDarkMode } = useDarkMode()
  let showDrawer = $state(false)
</script>

<div class="flex h-full flex-col">
  <div class="flex h-full">
    <Drawer open={showDrawer} side="left">
      <VerticalMenu></VerticalMenu>
    </Drawer>
    <div class="flex h-full w-full flex-col">
      <div class="flex justify-between p-4">
        <div class="flex">
          <button class="btn-icon" onclick={() => (showDrawer = !showDrawer)}>
            <i class="bx bx-menu text-[24px]"></i>
          </button>
        </div>
        <div class="flex"></div>
        <div class="flex gap-2">
          <HealthPopup></HealthPopup>
          <button
            onclick={toggleDarkMode}
            class={'btn-icon text-[24px] preset-tonal-surface ' +
              (darkMode.value === 'dark' ? 'bx bx-sun ' : 'bx bx-moon ')}
          ></button>
        </div>
      </div>
      {@render children()}
    </div>
  </div>
</div>
<GlobalModal></GlobalModal>
