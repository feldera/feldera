<script lang="ts">
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
    <div class="h-full w-full">
      <div class="flex justify-between p-4">
        <div class="flex">
          <button class="btn-icon" onclick={() => (showDrawer = !showDrawer)}>
            <i class="bx bx-menu text-[24px]"></i>
          </button>
        </div>
        <div class="flex">children</div>
        <div class="flex">
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
