<script lang="ts">
  import { beforeNavigate, goto } from '$app/navigation'
  import GenericDialog from '$lib/components/dialogs/GenericDialog.svelte'
  import { useGlobalDialog } from '$lib/compositions/layout/useGlobalDialog.svelte'

  const globalDialog = useGlobalDialog()
  let {
    unsavedChanges = $bindable(),
    save
  }: {
    unsavedChanges: boolean
    /** Push all dirty files upstream. */
    save?: () => void
  } = $props()

  type Resolution = 'save' | 'discard' | 'cancel'

  // Guard against navigating away with unsaved changes
  let navigationResolve: ((value: Resolution) => void) | null = $state(null)
  let suppressNavigationGuard = false

  beforeNavigate((navigation) => {
    if (!unsavedChanges || navigationResolve || suppressNavigationGuard) {
      return
    }
    const samePage = (() => {
      const fromUrl = navigation.from?.url
      const toUrl = navigation.to?.url
      return fromUrl && toUrl
        ? fromUrl.pathname === toUrl.pathname &&
            fromUrl.search === toUrl.search &&
            fromUrl.hash === toUrl.hash
        : false
    })()

    if (samePage) {
      // Skip the dialog if shallow routing to the same page
      return
    }
    navigation.cancel()

    globalDialog.dialog = unsavedChangesDialog

    new Promise<Resolution>((resolve) => {
      navigationResolve = resolve
    }).then((resolution) => {
      globalDialog.dialog = null
      if (resolution === 'cancel' || !navigation.to) {
        goto('').then(() => {
          navigationResolve = null
          suppressNavigationGuard = false
        })
        return
      }
      if (resolution === 'save') {
        save?.()
      }
      // Disposal of the pipeline's in-memory editor state is handled by the
      // $effect cleanup in PipelineCodePanel when `pipelineName` changes or
      // the component unmounts as a result of the navigation below. Running
      // it synchronously here would race with Svelte's reactive flush: the
      // push() above mutates pipelineCache, which re-derives `files` with a
      // new identity, and the CodeEditor effects that re-run will crash
      // dereferencing the just-disposed `openFiles[filePath]`.
      goto(navigation.to.url.pathname + navigation.to.url.search + navigation.to.url.hash).then(
        () => {
          navigationResolve = null
          suppressNavigationGuard = false
        }
      )
    })
  })

  // If autosave clears unsaved changes while the dialog is open, treat it
  // as "Save and continue" and proceed with navigation automatically.
  $effect(() => {
    if (!unsavedChanges && navigationResolve) {
      navigationResolve('save')
    }
  })

  // Warn before closing browser tab/window with unsaved changes.
  const handleBeforeUnload = (e: BeforeUnloadEvent) => {
    if (!unsavedChanges) {
      return
    }
    // Trigger the browser's native "unsaved changes" prompt. Assigning
    // returnValue is required by older browsers; calling preventDefault is
    // required by the modern spec.
    e.preventDefault()
    e.returnValue = ''
    // Close the in-app dialog path so only the native prompt shows.
    navigationResolve?.('cancel')
    navigationResolve = null
    suppressNavigationGuard = true
  }
</script>

<svelte:window onbeforeunload={handleBeforeUnload} />

{#snippet unsavedChangesDialog()}
  <GenericDialog
    content={{
      title: 'Unsaved changes',
      description:
        'You have unsaved changes in the pipeline code. Save them before continuing, or discard them to leave this pipeline.'
    }}
  >
    <div
      class="flex w-full flex-col-reverse gap-4 pt-2 sm:flex-row sm:justify-end"
      data-testid="box-dialog-actions"
    >
      <button
        class="btn preset-filled-surface-50-950 px-4"
        onclick={() => navigationResolve?.('cancel')}
        data-testid="btn-dialog-cancel"
      >
        Cancel
      </button>
      <button
        class="btn preset-outlined-error-500 px-4"
        onclick={() => navigationResolve?.('discard')}
        data-testid="btn-dialog-discard"
      >
        Discard changes
      </button>
      <button
        class="btn preset-filled-primary-500 px-4 font-semibold"
        onclick={() => navigationResolve?.('save')}
        data-testid="btn-dialog-save-and-continue"
      >
        Save and continue
      </button>
    </div>
  </GenericDialog>
{/snippet}
