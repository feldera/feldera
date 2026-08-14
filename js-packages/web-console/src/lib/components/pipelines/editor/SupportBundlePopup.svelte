<script lang="ts">
  /**
   * Opens a support bundle from disk in the profile viewer.
   *
   * Two shapes, one flow:
   *   - `mode="menu"` (the pipeline editor's split button): the trigger opens a
   *     dropdown offering the download, the "collect new data" toggle and the
   *     entry that picks a bundle.
   *   - `mode="pick"` (the home page's button): the trigger picks a bundle
   *     straight away, and the dropdown holds nothing but the confirmation.
   *
   * Confirming is a second click by design: `window.open` counts as a popup
   * unless it runs synchronously inside a click handler, and picking a file is
   * asynchronous. So picking only shows the confirmation, and the click on it
   * opens the tab.
   *
   * A picked bundle is remembered in the bundle history, so the viewer tab can
   * read it again — including after a reload. Browsers without the File System
   * Access API fall back to the hidden file input below, whose file the history
   * keeps a copy of; only a bundle too big to copy is handed over as bytes, once.
   */
  import { slide } from 'svelte/transition'
  import Popup from '$lib/components/common/Popup.svelte'
  import SlidingPanels from '$lib/components/common/SlidingPanels.svelte'
  import { openStoredBundleTab, openUploadBundleTab } from '$lib/compositions/profileBundleHandoff'
  import { type PickedBundle, useBundlePicker } from '$lib/compositions/useBundlePicker.svelte'
  import { useToast } from '$lib/compositions/useToastNotification'
  import type { Snippet } from '$lib/types/svelte'
  import SupportBundleConfirm from './SupportBundleConfirm.svelte'
  import SupportBundleMenu from './SupportBundleMenu.svelte'

  type Props = {
    trigger: Snippet<[toggle: () => void, isOpen: boolean]>
    /** Whether the trigger opens the bundle menu or picks a bundle right away. */
    mode?: 'menu' | 'pick'
    /** Which edge of the trigger the dropdown hangs from. */
    align?: 'left' | 'right'
    /** Menu mode only; when omitted the menu offers no download. */
    onDownload?: () => void
    collectNewData?: boolean
    downloadLabel?: string
    pickLabel?: string
    confirmLabel?: string
    disabled?: boolean
    wrapperClass?: string
  }

  let {
    trigger,
    mode = 'menu',
    align = 'right',
    onDownload,
    collectNewData = $bindable(false),
    downloadLabel,
    pickLabel,
    confirmLabel = 'View profile',
    disabled = false,
    wrapperClass
  }: Props = $props()

  const toast = useToast()
  const picker = useBundlePicker()

  let fileInput: HTMLInputElement | null = $state(null)
  let picked: PickedBundle | null = $state(null)
  let showDropdown = $state(false)

  const reportError = (scope: string) => (e: unknown) =>
    toast.toastError(scope)(e instanceof Error ? e : new Error(String(e)), 8000)

  /** Shows the confirmation for a bundle the user chose. */
  function confirmPicked(bundle: PickedBundle) {
    picked = bundle
    showDropdown = true
  }

  async function pickBundle() {
    if (!picker.isSupported) {
      // The click on the input closes an open dropdown, since the input sits
      // outside it; confirming the pick opens it again.
      fileInput?.click()
      return
    }
    try {
      const bundle = await picker.pick()
      if (bundle) {
        confirmPicked(bundle)
      }
    } catch (e) {
      reportError('Opening support bundle')(e)
    }
  }

  /** Drops the pick: back to the menu where there is one, otherwise away. */
  function dismissPicked() {
    picked = null
    if (mode === 'pick') {
      showDropdown = false
    }
  }

  /**
   * Opens the viewer for the confirmed bundle. Runs inside the confirming click,
   * so `window.open` is allowed.
   */
  function openViewerTab() {
    const bundle = picked
    picked = null
    if (!bundle) {
      return
    }

    if (bundle.bundleId !== undefined) {
      // The viewer reads the bundle out of the history itself, so nothing has to
      // be handed over and its tab survives a reload.
      openStoredBundleTab(bundle.bundleId)
      return
    }

    let handoff: ReturnType<typeof openUploadBundleTab>
    try {
      handoff = openUploadBundleTab()
    } catch (e) {
      reportError('Opening support bundle viewer')(e)
      return
    }
    ;(async () => {
      try {
        const bytes = await bundle.read()
        await handoff.send(bytes.buffer as ArrayBuffer)
      } catch (e) {
        handoff.cancel()
        reportError('Opening support bundle viewer')(e)
      }
    })()
  }
</script>

<!-- Outside the dropdown on purpose: in menu mode the dropdown closes the moment
     the input is clicked, and an unmounted input can no longer report the file. -->
<input
  type="file"
  accept=".zip"
  bind:this={fileInput}
  onchange={async (e) => {
    const file = (e.currentTarget as HTMLInputElement).files?.[0]
    if (file) {
      ;(e.currentTarget as HTMLInputElement).value = ''
      confirmPicked(await picker.fromFile(file))
    }
  }}
  class="hidden"
  data-testid="input-upload-support-bundle"
/>

<Popup
  {wrapperClass}
  bind:open={showDropdown}
  trigger={mode === 'pick' ? pickTrigger : trigger}
  content={dropdown}
/>

<!-- In pick mode the trigger picks instead of toggling, and the dropdown appears
     on its own once there is something to confirm. -->
{#snippet pickTrigger(_toggle: () => void, isOpen: boolean)}
  {@render trigger(pickBundle, isOpen)}
{/snippet}

{#snippet dropdown(close: () => void)}
  <div
    transition:slide={{ duration: 100 }}
    class="bg-white-dark absolute top-10 z-30 flex min-w-[220px] flex-col overflow-hidden rounded shadow-md {align ===
    'right'
      ? 'right-0'
      : 'left-0'}"
    data-testid="box-support-bundle-menu"
  >
    {#if mode === 'pick'}
      {@render confirmPage()}
    {:else}
      <SlidingPanels
        current={picked ? 'confirm' : 'menu'}
        pages={[
          { key: 'menu', content: menuPage },
          { key: 'confirm', content: confirmPage }
        ]}
      />
    {/if}
  </div>

  {#snippet menuPage()}
    <SupportBundleMenu
      bind:collectNewData
      onDownload={onDownload &&
        (() => {
          // Downloading hands the interaction to a dialog, so the dropdown goes away.
          close()
          onDownload()
        })}
      onPickBundle={pickBundle}
      {disabled}
      {downloadLabel}
      {pickLabel}
    />
  {/snippet}

  <!-- Confirmation view: opens the viewer tab on a direct click so the browser
       preserves user activation through window.open. -->
  {#snippet confirmPage()}
    {#if picked}
      <SupportBundleConfirm
        name={picked.name}
        {confirmLabel}
        onConfirm={() => {
          openViewerTab()
          close()
        }}
        onDismiss={dismissPicked}
      />
    {/if}
  {/snippet}
{/snippet}
