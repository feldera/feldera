<script lang="ts">
  import type { Runtime, Module, Define } from '@observablehq/runtime'
  import { Inspector } from '@observablehq/runtime'

  let {
    runtime,
    define,
    onModuleUpdated,
    onDelete,
    isLast
  }: {
    runtime: Runtime
    define: Define
    onModuleUpdated: () => void
    onDelete: () => void
    isLast: boolean
  } = $props()

  const introspectNotebook = (module: Define) => {
    let schema: string[] = []
    module(
      {
        module: () => ({
          variable: (name: string) => {
            if (name.includes('viewof')) {
              schema.push(name)
            }
            return { define: () => {} }
          }
        })
      },
      (x: string) => x as any
    )
    return schema
  }

  let schema = $derived(introspectNotebook(define))
  let cells = $state<{ name: string; ref: HTMLElement }[]>([])
  let expanded = $state(true)

  $effect.root(() => {
    cells = schema.map((name) => ({ name, ref: undefined! }))
    setTimeout(() => {
      const module = runtime.module(define, (name) => {
        const found = cells.find((ref) => name === ref.name)?.ref
        if (!found) {
          return
        }
        return new Inspector(found)
      })
    })
  })

  const handleUpdate = (cellIdx: number) => {
    if (cellIdx === 0) {
      return
    }
    onModuleUpdated()
  }
</script>

<div class="group flex flex-nowrap overflow-hidden pr-8 pt-0.5 sm:pr-16">
  <button
    aria-label="expand"
    class="bg-red flex w-full max-w-8 justify-start text-[20px] group-hover:text-surface-400-600 hover:group-hover:text-surface-950-50 {expanded
      ? 'text-transparent'
      : 'text-surface-400-600'}"
    onclick={() => {
      // modules[pipelineName][i].expanded = !module.expanded
      expanded = !expanded
    }}><div class="fd fd-chevron-down" class:-rotate-90={expanded}></div></button
  >
  <div class="-mt-1 flex h-auto flex-col text-transparent">
    <!-- <button class="fd fd fd-plus group-hover:text-surface-400-600 text-[20px]"></button> -->
    <div class="w-5 flex-1 group-hover:bg-surface-50-950">z</div>
    <!-- <button class="fd fd fd-plus group-hover:text-surface-400-600 -mb-5 text-[20px]"></button> -->
  </div>
  <div class="w-full">
    {#each cells as { ref, name }, j}
      <div
        bind:this={cells[j].ref}
        class="mr-0.5 w-full"
        class:h-0={!expanded && j !== 0}
        onupdate={() => handleUpdate(j)}
      ></div>
    {/each}
  </div>
  {#if true}
    <button
      class:text-transparent={isLast}
      class:pointer-events-none={isLast}
      class="fd fd-trash-2 self-start p-2 text-[20px]"
      aria-label="delete"
      onclick={onDelete}
    ></button>
  {/if}
</div>

<style lang="scss" global>
  :global(.observablehq--running) {
    @apply h-0;
  }
</style>
