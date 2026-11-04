<script lang="ts">
  /**
   * The "Open support bundle" dialog. Put it in `useGlobalDialog().dialog` to show it.
   *
   * It lists the bundles the user opened or uploaded before, and the button at the bottom
   * left of it uploads a new one from disk into that list. A click on a bundle opens a
   * profile viewer tab, and that tab reads the archive out of the bundle history itself.
   */
  import GenericDialog from '#lib/components/dialogs/GenericDialog.svelte'
  import { useElapsedTime } from '#lib/compositions/common/useElapsedTime.ts'
  import { useGlobalDialog } from '#lib/compositions/layout/useGlobalDialog.svelte.ts'
  import { openStoredBundleTab } from '#lib/compositions/profileBundleHandoff.ts'
  import { useSupportBundleHistory } from '#lib/compositions/useSupportBundleHistory.svelte.ts'
  import { useToast } from '#lib/compositions/useToastNotification.ts'
  import {
    type BundleHistoryEntry,
    clearBundleHistory,
    isBundleCacheRequired,
    markBundleOpenedNow
  } from '#lib/services/supportBundleHistory.ts'
  import SupportBundlePopup from './SupportBundlePopup.svelte'

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
   * may have to be asked for it again. Granting permission to read a file uses up the click
   * that asked it, and opening a tab afterwards then depends on the browser extending
   * the new permission for that.
   *
   * An entry with no `requestPermission` is one stored as a copy of the archive, which
   * this app can read without asking. Nothing is awaited for such an entry, so
   * `window.open` runs inside the click that called this, which is the only way Safari
   * lets it through.
   *
   * The dialog stays open whatever fails, so that the user can try again.
   */
  const openBundle = async (entry: BundleHistoryEntry) => {
    const report = toast.toastError('Opening support bundle')
    let granted: boolean
    try {
      // If the user already gave permission, the browser shows no prompt and returns
      // true immediately. `requestPermission` throws, instead of returning false, in two
      // cases: this click was already used for an earlier prompt, or the file is gone.
      // In this case, an error popup is shown.
      granted = !entry.ops.requestPermission || (await entry.ops.requestPermission())
    } catch (e) {
      report(e instanceof Error ? e : new Error(String(e)), 8000)
      return
    }
    if (!granted) {
      report(
        new Error(
          `Reading ${entry.name} needs access to the file. Click it again and allow ` +
            'access when the browser asks, or open it from disk with "Upload support bundle".'
        ),
        8000
      )
      return
    }
    try {
      openStoredBundleTab(entry.id)
    } catch (e) {
      report(e instanceof Error ? e : new Error(String(e)), 8000)
      return
    }
    markBundleOpenedNow(entry.id).catch((e) =>
      console.warn('Failed to update the support bundle history:', e)
    )
    closeDialog()
  }

  const clearHistory = () =>
    clearBundleHistory().catch((e) =>
      console.warn('Failed to clear the support bundle history:', e)
    )

  /**
   * When the browser does not provide file handles, the history keeps copies of the
   * archives instead. The title says so, because those copies occupy the browser storage
   * until the user clears the history.
   */
  /** Whether a picked bundle waits for confirmation in the footer. */
  let confirming = $state(false)

  const historyTitle = isBundleCacheRequired()
    ? 'Recent support bundles (cached in the browser)'
    : 'Recent support bundles'
</script>

<GenericDialog content={{ title: historyTitle }}>
  <!-- As wide as the dialog and no wider, however long the names are, so that the
       list scrolls up and down only. -->
  <div
    class="scrollbar flex max-h-[50vh] w-full min-w-0 flex-col overflow-y-auto"
    data-testid="box-all-bundles"
  >
    {#each history.current as entry (entry.id)}
      <!-- The page's row hover is darker than the dialog in dark mode, so the row tints
           with a translucent grey that shows on either. -->
      <button
        class="group flex min-w-0 items-baseline justify-between gap-6 rounded px-2 py-2 text-left hover:bg-surface-950-50/[0.06] focus-visible:bg-surface-950-50/[0.06]"
        title={entry.name}
        onclick={() => openBundle(entry)}
        data-testid="btn-open-bundle-from-list"
      >
        <!-- A name too long for its row ends in an ellipsis. The `title` above
             carries the whole name, for the tooltip. -->
        <span class="min-w-0 truncate" data-testid="box-bundle-name">{entry.name}</span>
        <!-- Keeps its width and its place at the row's right edge. The name gives up
             the space instead. On hover the time gives way to what a click does; both
             share one grid cell, so the row keeps its width either way. -->
        <span class="grid shrink-0 justify-items-end whitespace-nowrap">
          <span
            class="col-start-1 row-start-1 text-surface-700-300 group-hover:invisible group-focus-visible:invisible"
            data-testid="box-bundle-opened-ago"
          >
            {formatElapsedTime(new Date(entry.openedAt), 'dhm').trim()} ago
          </span>
          <span
            class="invisible col-start-1 row-start-1 font-medium group-hover:visible group-focus-visible:visible"
            aria-hidden="true"
          >
            Open profile
          </span>
        </span>
      </button>
    {:else}
      <!-- What a support bundle is and where to get one, from the operations guide. -->
      <div class="flex flex-col items-center gap-2 px-6 py-6 text-center">
        <span
          class="fd fd-package-open flex h-10 w-10 items-center justify-center rounded-lg bg-surface-950-50/[0.06] text-[20px] text-surface-700-300"
        ></span>
        <span class="font-medium text-surface-950-50">No support bundles opened recently</span>
        <span class="max-w-[420px] text-sm text-surface-600-400">
          A support bundle is a zip of a pipeline's logs, configuration, metrics and circuit
          profiles. Download one from a pipeline's <b class="font-medium">View profile</b> menu or
          with <code class="font-dm-mono">fda support-bundle</code>, then upload it here to explore
          it.
        </span>
        <a
          class="text-sm text-surface-950-50 underline underline-offset-2 hover:text-surface-600-400"
          href="https://docs.feldera.com/operations/guide/#diagnosing-performance-issues"
          target="_blank"
          rel="noreferrer">Learn more about support bundles</a
        >
      </div>
    {/each}
  </div>
  <div class="flex {history.current.length ? 'justify-between' : 'justify-center'}">
    <!-- This button opens the file picker immediately. The picked bundle then heads the
         list above, and a click on it opens the viewer tab: the browser opens a tab only
         from a click of its own, a "user activation", and the pick used this one up.
         A bundle the history could not keep is confirmed in the button's place. -->
    <SupportBundlePopup mode="pick" bind:confirming onOpened={closeDialog}>
      {#snippet trigger(pick)}
        <button
          class="btn h-8 {history.current.length
            ? 'preset-outlined-primary-500'
            : 'preset-filled-primary-500'}"
          onclick={pick}
          data-testid="btn-pick-support-bundle"
        >
          <span class="fd fd-arrow-up text-[16px]"></span>
          Upload support bundle
        </button>
      {/snippet}
    </SupportBundlePopup>
    <!-- Only offered when there is something to clear, and not while the confirmation
         needs the footer's width. -->
    {#if history.current.length && !confirming}
      <button
        class="btn h-8 preset-outlined-surface-200-800"
        onclick={clearHistory}
        data-testid="btn-clear-bundle-history"
      >
        Clear history
      </button>
    {/if}
  </div>
</GenericDialog>
