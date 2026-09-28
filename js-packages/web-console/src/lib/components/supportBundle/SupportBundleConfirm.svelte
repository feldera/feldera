<script lang="ts">
  /**
   * A confirmation popup for the chosen support bundle. It is shown wherever opening a
   * bundle needs a second click, that is wherever the browser needs a so-called "user
   * activation": a user action, usually a button click, that the browser reads as
   * explicit confirmation before it lets a script do something invasive, such as
   * opening a new page.
   *
   * `variant` adjusts appearance based on the parent container:
   * `popup` is shown in a dropdown under whatever was clicked,
   * `page` - in the middle of an empty page.
   * The two take different props, e.g. `onDismiss`, since only the dropdown has a menu to navigate back to.
   */
  type Props = {
    name: string
    confirmLabel?: string
    /** Runs inside the click on the button, so it may call `window.open`. */
    onConfirm: () => void
    'data-testid'?: string
  } & ({ variant?: 'popup'; onDismiss?: () => void } | { variant: 'page'; onDismiss?: never })

  let {
    name,
    confirmLabel = 'View profile',
    onConfirm,
    onDismiss,
    variant = 'popup',
    'data-testid': testid = 'btn-confirm-view-profile'
  }: Props = $props()
</script>

{#if variant === 'page'}
  <div class="flex flex-col items-center gap-3" data-testid="box-support-bundle-confirm">
    <span class="max-w-full truncate font-semibold" title={name}>{name}</span>
    <button class="btn preset-filled-primary-500" onclick={onConfirm} data-testid={testid}>
      <span class="fd fd-file-search text-[18px]"></span>
      <span>{confirmLabel}</span>
    </button>
  </div>
{:else}
  <div class="flex flex-col" data-testid="box-support-bundle-confirm">
    <div class="flex items-center gap-2 px-2 py-2">
      {#if onDismiss}
        <button
          class="btn-icon h-7 w-7"
          onclick={onDismiss}
          aria-label="Dismiss"
          title="Choose another bundle"
        >
          <span class="fd fd-chevron-left text-[20px]"></span>
        </button>
      {/if}
      <span class="min-w-0 flex-1 truncate text-sm" title={name}>{name}</span>
    </div>
    <div class="px-2 pb-2">
      <button
        class="btn h-8! w-full preset-filled-primary-500"
        onclick={onConfirm}
        data-testid={testid}
      >
        <span class="fd fd-file-search text-[18px]"></span>
        <span>{confirmLabel}</span>
      </button>
    </div>
  </div>
{/if}
