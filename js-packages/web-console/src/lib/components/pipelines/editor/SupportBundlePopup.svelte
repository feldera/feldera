<script lang="ts">
  /**
   * The support bundle dropdown: the download entry, the "collect new data" toggle,
   * and the entry that opens a bundle from disk in the profile viewer.
   *
   * Confirming a pick takes a second click: the browser treats `window.open` as a
   * popup unless it runs synchronously inside a click handler, and picking a file is
   * asynchronous. So picking shows the confirmation, and the click on it opens the
   * tab.
   *
   * A picked bundle goes into the bundle history, so the viewer tab can read it
   * again, including after a reload. Browsers without the File System Access API fall
   * back to the hidden file input below, whose file the history copies; only a bundle
   * too big to copy is handed over as bytes, once.
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
    /** When omitted the menu offers no download. */
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
      // The input sits outside the dropdown, so clicking it closes an open dropdown;
      // confirming the pick opens the dropdown again.
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

  /** Drops the pick and goes back to the menu. */
  function dismissPicked() {
    picked = null
  }

  /**
   * Opens the viewer for the confirmed bundle. Runs inside the confirming click, so
   * `window.open` is allowed.
   */
  function openViewerTab() {
    const bundle = picked
    picked = null
    if (!bundle) {
      return
    }

    if (bundle.bundleId !== undefined) {
      // The viewer reads the bundle out of the history itself, so nothing has to be
      // handed over and the viewer tab survives a reload.
      try {
        openStoredBundleTab(bundle.bundleId)
      } catch (e) {
        reportError('Opening support bundle viewer')(e)
      }
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

<!-- The input sits outside the dropdown: the dropdown closes the moment the input is
     clicked, and an unmounted input reports no file. -->
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

<Popup {wrapperClass} bind:open={showDropdown} {trigger} content={dropdown} />

{#snippet dropdown(close: () => void)}
  <div
    transition:slide={{ duration: 100 }}
    class="bg-white-dark absolute top-10 right-0 z-30 flex min-w-[220px] flex-col overflow-hidden rounded shadow-md"
    data-testid="box-support-bundle-menu"
  >
    <SlidingPanels
      current={picked ? 'confirm' : 'menu'}
      pages={[
        { key: 'menu', content: menuPage },
        { key: 'confirm', content: confirmPage }
      ]}
    />
  </div>

  {#snippet menuPage()}
    <SupportBundleMenu
      bind:collectNewData
      onDownload={onDownload &&
        (() => {
          // Downloading opens a dialog, so the dropdown closes.
          close()
          onDownload()
        })}
      onPickBundle={pickBundle}
      {disabled}
      {downloadLabel}
      {pickLabel}
    />
  {/snippet}

  <!-- The confirmation opens the viewer tab on a direct click, so the browser keeps
       user activation through `window.open`. -->
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
