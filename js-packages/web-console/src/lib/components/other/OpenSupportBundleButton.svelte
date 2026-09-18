<script lang="ts">
  /**
   * "Open support bundle": the button and the dialog it opens.
   *
   * The dialog lists the bundles the user opened before, and the button at the bottom
   * left of it picks a new one from disk. Both open a profile viewer tab, and that tab
   * reads the archive out of the bundle history itself.
   */
  import GenericDialog from '$lib/components/dialogs/GenericDialog.svelte'
  import SupportBundlePopup from '$lib/components/pipelines/editor/SupportBundlePopup.svelte'
  import { useElapsedTime } from '$lib/compositions/common/useElapsedTime'
  import { useGlobalDialog } from '$lib/compositions/layout/useGlobalDialog.svelte'
  import { openStoredBundleTab } from '$lib/compositions/profileBundleHandoff'
  import {
    type SupportBundleEntry,
    useSupportBundleHistory
  } from '$lib/compositions/useSupportBundleHistory.svelte'
  import { useToast } from '$lib/compositions/useToastNotification'
  import { isBundleCacheRequired } from '$lib/services/supportBundleCache'

  const {
    btnClass,
    onOpen
  }: {
    btnClass?: string
    /** Runs when the dialog opens. */
    onOpen?: () => void
  } = $props()

  const history = useSupportBundleHistory()
  const globalDialog = useGlobalDialog()
  const toast = useToast()
  const { formatElapsedTime } = useElapsedTime()

  const closeDialog = () => {
    globalDialog.dialog = null
  }

  /**
   * Opens a bundle from the list in a new profile viewer tab, which reads the file
   * through the handle stored with the history entry.
   *
   * Browsers forget permission to read a file from one visit to the next, so the user
   * may have to be asked for it again first. Answering that question uses up the click
   * that asked it, and opening a tab afterwards then depends on the browser counting
   * the answer as a user action of its own. A browser that does not count it blocks
   * the new tab, and `openStoredBundleTab` reports that. The fix would be a second
   * click to confirm, the way `SupportBundlePopup` asks for one after a file is picked.
   *
   * The dialog stays open whatever fails, so that the user can try again.
   */
  const openBundle = async (bundle: SupportBundleEntry) => {
    const report = toast.toastError('Opening support bundle')
    let granted: boolean
    try {
      // `requestPermission` rejects, rather than answering false, when the click that
      // would justify the question has already been used up, and when the handle no
      // longer points at anything.
      granted = !bundle.needsPermission || (await history.grantAccess(bundle))
    } catch (e) {
      report(e instanceof Error ? e : new Error(String(e)), 8000)
      return
    }
    if (!granted) {
      report(
        new Error(
          `Reading ${bundle.name} needs access to the file. Click it again and allow ` +
            'access when the browser asks, or open it from disk with "Upload support bundle".'
        ),
        8000
      )
      return
    }
    try {
      openStoredBundleTab(bundle.id)
    } catch (e) {
      report(e instanceof Error ? e : new Error(String(e)), 8000)
      return
    }
    history.touch(bundle.id)
    closeDialog()
  }

  /**
   * Where the browser hands out no file handles, the history keeps copies of the
   * archives instead. The title says so, because those copies occupy the storage this
   * site is allowed until the user clears the history.
   */
  const historyTitle = isBundleCacheRequired()
    ? 'Recent pipeline profiles (cached in the browser)'
    : 'Recent pipeline profiles'
</script>

<button
  class="btn h-9 preset-outlined-primary-500 {btnClass}"
  onclick={() => {
    globalDialog.dialog = openBundleDialog
    onOpen?.()
  }}
  data-testid="btn-open-support-bundle"
>
  Open support bundle
</button>

{#snippet openBundleDialog()}
  <GenericDialog content={{ title: historyTitle }}>
    <!-- As wide as the dialog and no wider, however long the names are, so that the
         list scrolls up and down only. -->
    <div
      class="scrollbar flex max-h-[50vh] w-full min-w-0 flex-col overflow-y-auto"
      data-testid="box-all-bundles"
    >
      {#each history.current as bundle (bundle.id)}
        <button
          class="flex min-w-0 items-baseline justify-between gap-6 rounded px-2 py-2 text-left hover:preset-tonal-surface"
          title={bundle.name}
          onclick={() => openBundle(bundle)}
          data-testid="btn-open-bundle-from-list"
        >
          <!-- A name too long for its row ends in an ellipsis. The `title` above
               carries the whole name, for the tooltip. -->
          <span class="min-w-0 truncate" data-testid="box-bundle-name">{bundle.name}</span>
          <!-- Keeps its width and its place at the row's right edge. The name gives up
           the space instead. -->
          <span
            class="shrink-0 whitespace-nowrap text-surface-700-300"
            data-testid="box-bundle-opened-ago"
          >
            {formatElapsedTime(new Date(bundle.openedAt), 'dhm').trim()} ago
          </span>
        </button>
      {:else}
        <span class="px-2 py-2 text-surface-700-300">No support bundles opened recently</span>
      {/each}
    </div>
    <div class="flex justify-between">
      <!-- This button picks a bundle straight away, and its popup holds nothing but
           the confirmation, whose click is what lets the viewer tab be opened. -->
      <SupportBundlePopup mode="pick" align="left" drop="up" onOpened={closeDialog}>
        {#snippet trigger(pick)}
          <button
            class="btn preset-outlined-primary-500"
            onclick={pick}
            data-testid="btn-pick-support-bundle"
          >
            <span class="fd fd-file-search text-[18px]"></span>
            Upload support bundle
          </button>
        {/snippet}
      </SupportBundlePopup>
      <button
        class="btn preset-tonal-surface"
        onclick={() => history.clear()}
        data-testid="btn-clear-bundle-history"
      >
        Clear history
      </button>
    </div>
  </GenericDialog>
{/snippet}
