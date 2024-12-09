<script lang="ts">
  import type { SystemError } from '$lib/compositions/health/systemErrors'
  import type { Loadable } from '@square/svelte-store'
  import InlineDropdown from '$lib/components/common/InlineDropdown.svelte'
  import ErrorTile from '$lib/components/health/ErrorTile.svelte'
  import { slide } from 'svelte/transition'

  const { systemErrors, close }: { systemErrors: Loadable<SystemError[]>; close?: () => void } =
    $props()
</script>

<div class="flex flex-col gap-2 p-4">
  <span class="h5 font-medium">Feldera Health</span>
  <div class="h-full">
    {#each $systemErrors as systemError}
      <div class="mb-5">
        <InlineDropdown>
          {#snippet header(open, toggle)}
            <div class="">
              <div
                class="flex w-full cursor-pointer items-center gap-2 py-2"
                onclick={toggle}
                role="presentation"
              >
                <div
                  class={'fd fd-chevron-down text-[20px] transition-transform ' +
                    (open ? 'rotate-180' : '')}
                ></div>
                <a
                  href={systemError.cause.source}
                  class="text-primary-500"
                  onclick={(e) => {
                    close?.()
                  }}
                >
                  {systemError.name}
                </a>
              </div>
              {#if !open}
                <div
                  class=" -mb-5 w-full overflow-x-hidden overflow-y-clip overflow-ellipsis whitespace-nowrap text-sm"
                >
                  {systemError.message}
                </div>
              {/if}
            </div>
          {/snippet}
          {#snippet content()}
            <div transition:slide={{ duration: 150 }}>
              <ErrorTile {systemError}></ErrorTile>
            </div>
          {/snippet}
        </InlineDropdown>
      </div>
    {/each}
  </div>
</div>
