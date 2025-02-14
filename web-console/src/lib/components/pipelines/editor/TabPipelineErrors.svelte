<script lang="ts">
  import { useSkeletonTheme } from '$lib/compositions/useSkeletonTheme.svelte'
  import type { SystemError } from '$lib/compositions/health/systemErrors'

  let { errors }: { errors: SystemError<any, any>[] } = $props()

  const theme = useSkeletonTheme()
</script>

<div class="{errors.length ? '' : ''} h-full overflow-y-auto scrollbar">
  <div class=" flex min-h-full flex-col gap-4 rounded sm:pt-4">
    {#each errors as systemError}
      <div class="bg-white-dark whitespace-nowrap rounded p-4">
        <a
          class=""
          href={systemError.cause.source}
          aria-label={systemError.cause.warning ? 'Warning location' : 'Error location'}
        >
          <span
            class=" text-[20px] {systemError.cause.warning
              ? 'fd fd-triangle-alert text-warning-500'
              : 'fd fd-circle-x text-error-500'}"
          >
          </span></a
        >
        <span
          class="whitespace-pre-wrap break-words text-start align-text-top leading-none"
          style="font-family: {theme.config.monospaceFontFamily}"
        >
          {systemError.message}
        </span>
      </div>
    {:else}
      <span class="text-surface-600-400">No errors</span>
    {/each}
  </div>
</div>
