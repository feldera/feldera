<script lang="ts">
  /**
   * Opens a support bundle from disk in the profile viewer.
   *
   * The same flow in two shapes:
   *   - `mode="menu"`, the pipeline editor's split button: the trigger opens a
   *     dropdown offering the download, the "collect new data" toggle and the entry
   *     that picks a bundle.
   *   - `mode="pick"`, the "Open support bundle" dialog's button: the trigger picks a
   *     bundle straight away, and the dropdown holds nothing but the confirmation.
   *
   * Opening the viewer takes a second click. A browser opens a new tab only while it
   * is handling a user action, which `SupportBundleConfirm.svelte` describes, and
   * choosing a file takes the user as long as it takes, so the click that started the
   * picking is long over by the time there is a bundle to open. Picking therefore
   * shows a confirmation, and the click on that confirmation opens the tab.
   *
   * A picked bundle goes into the bundle history, so that the viewer tab can read it
   * again, including after a reload. A browser without `showOpenFilePicker` falls back
   * to the hidden `<input type=file>` below, and the history keeps a copy of the file
   * it yields. Only a bundle too large to copy is handed to the viewer as bytes, once.
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
    /**
     * Which way the dropdown opens. Use `'up'` for a trigger near the bottom edge of
     * its container, where a downward dropdown would hang off.
     */
    drop?: 'down' | 'up'
    /** Runs once the viewer tab is open, so a caller such as a dialog can close itself. */
    onOpened?: () => void
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
    drop = 'down',
    onOpened,
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

  /**
   * Forgets the picked bundle. In menu mode that brings the menu back, in pick mode
   * there is nothing else to show, so the dropdown closes.
   */
  function dismissPicked() {
    picked = null
    if (mode === 'pick') {
      showDropdown = false
    }
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
      onOpened?.()
      return
    }

    let handoff: ReturnType<typeof openUploadBundleTab>
    try {
      handoff = openUploadBundleTab()
    } catch (e) {
      reportError('Opening support bundle viewer')(e)
      return
    }
    // The transfer below outlives this component if `onOpened` unmounts it: the
    // handoff is a closure over the opened window, not component state.
    onOpened?.()
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

<!-- The input is outside the dropdown on purpose: in menu mode the dropdown closes the
     moment the input is clicked, and an input that has been unmounted never reports
     the file the user chose. -->
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

<!-- In pick mode the trigger picks instead of toggling, and the dropdown opens by
     itself once there is something to confirm. -->
{#snippet pickTrigger(_toggle: () => void, isOpen: boolean)}
  {@render trigger(pickBundle, isOpen)}
{/snippet}

{#snippet dropdown(close: () => void)}
  <div
    transition:slide={{ duration: 100 }}
    class="bg-white-dark absolute z-30 flex min-w-[220px] flex-col overflow-hidden rounded shadow-md {align ===
    'right'
      ? 'right-0'
      : 'left-0'} {drop === 'up' ? 'bottom-10' : 'top-10'}"
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
