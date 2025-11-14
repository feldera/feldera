<script lang="ts">
  // https://codesandbox.io/p/sandbox/drawer-with-tailwind-css-48z1k3
  import type { Snippet } from '$lib/types/svelte'
  const openClassNames = {
    right: 'translate-x-0',
    left: 'translate-x-0',
    top: 'translate-y-0',
    bottom: 'translate-y-0'
  }

  const closeClassNames = {
    right: 'translate-x-full',
    left: '-translate-x-full',
    top: '-translate-y-full',
    bottom: 'translate-y-full'
  }

  const classNames = {
    right: 'inset-y-0 right-0',
    left: 'inset-y-0 left-0',
    top: 'inset-x-0 top-0',
    bottom: 'inset-x-0 bottom-0'
  }

  let {
    open = $bindable(),
    side,
    children,
    class: _class,
    width,
    modal
  }: {
    open: boolean
    side: keyof typeof classNames
    children?: Snippet
    class: string
    width: string
    modal: boolean
  } = $props()
</script>

{#snippet drawerContent()}
  <div class={'pointer-events-none fixed z-40 max-w-full ' + classNames[side]}>
    <div
      role="presentation"
      class={'pointer-events-auto relative h-full w-full transform transition duration-300 ease-in-out ' +
        (open ? openClassNames[side] : closeClassNames[side])}
      onclick={(event) => {
        event.stopPropagation()
      }}
    >
      <div
        role="dialog"
        aria-modal="true"
        class={'flex h-full flex-col' + (open ? ' shadow-xl ' : ' ') + `${width} ` + _class}
      >
        {@render children?.()}
      </div>
    </div>
  </div>
{/snippet}

{#if modal}
  <div
    role="presentation"
    class="relative z-40"
    onclick={() => {
      open = !open
    }}
  >
    <div
      class={'fixed inset-0 bg-gray-500 bg-opacity-75 transition-all duration-300 ease-in-out ' +
        (open ? 'visible opacity-100 ' : 'invisible opacity-0')}
    ></div>
    <div class={open ? 'fixed inset-0 overflow-hidden' : ''}>
      <div class="absolute inset-0 overflow-hidden">
        {@render drawerContent()}
      </div>
    </div>
  </div>
{:else}
  {@render drawerContent()}
{/if}
