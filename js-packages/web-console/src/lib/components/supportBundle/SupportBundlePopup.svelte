<script lang="ts">
  /**
   * The support bundle dropdown in the pipeline editor: the download entry, the "collect new data" toggle,
   * and the button to upload a bundle from disk.
   *
   * A profile from the  bundle is opened in a new window. When the bundle is chosen from disk
   * the "user activation" - a browser term - from a click is lost because the click is used
   * to open the system file picker dialog. When the user picks the bundle file in the system dialog
   * the browser requires a fresh "user activation" to open a new browser page.
   * It is achieved through a second click - on a confirmation button that appears in the popup
   * together with the filename of the picked bundle (<SupportBundleConfirm>), after the system dialog is closed.
   *
   * A picked bundle goes into the bundle history, so that the viewer tab can read it
   * again.
   */
  import { slide } from 'svelte/transition'
  import Popup from '$lib/components/common/Popup.svelte'
  import SlidingPanels from '$lib/components/common/SlidingPanels.svelte'
  import { openStoredBundleTab, openUploadBundleTab } from '$lib/compositions/profileBundleHandoff'
  import { type PickedBundle, useBundlePicker } from '$lib/compositions/useBundlePicker'
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
      // Clicking the input closes the dropdown, because the input sits outside it.
      // `confirmPicked` opens the dropdown again once there is a file to confirm.
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

  /** Forgets the picked bundle, which brings the menu back. */
  function dismissPicked() {
    picked = null
  }

  /**
   * Opens the viewer tab for the confirmed bundle. This runs inside the click on the
   * confirmation, which is what allows it to call `window.open`.
   */
  function openViewerTab() {
    const bundle = picked
    picked = null
    if (!bundle) {
      return
    }

    if (bundle.bundleId !== undefined) {
      // The viewer reads the bundle out of the history itself, so nothing has to be
      // handed from this tab to that one, and the viewer tab survives a reload.
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

<!-- The input is outside the dropdown on purpose: the dropdown closes the moment the
     input is clicked, and an input that has been unmounted never reports the file the
     user chose. -->
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

<Popup {wrapperClass} bind:isOpen={showDropdown} {trigger} content={dropdown} />

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

  <!-- The confirmation opens the viewer tab from the click on it, which is what the
       browser requires before it allows `window.open`. -->
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
