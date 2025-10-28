<script lang="ts">
  import { beforeNavigate, goto } from '$app/navigation'
  import { useGlobalDialog } from '$lib/compositions/layout/useGlobalDialog.svelte'
  import DeleteDialog, { deleteDialogProps } from '$lib/components/dialogs/DeleteDialog.svelte'

  const globalDialog = useGlobalDialog()
  let { unsavedChanges = $bindable(), cleanup }: { unsavedChanges: boolean; cleanup?: () => void } =
    $props()

  // Guard against navigating away with unsaved changes
  let navigationResolve: ((value: boolean) => void) | null = $state(null)
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
    globalDialog.onclose = () => {
      globalDialog.dialog = null
      navigationResolve?.(false)
    }

    new Promise<boolean>((resolve) => {
      navigationResolve = resolve
    }).then((shouldNavigate) => {
      globalDialog.dialog = null
      if (shouldNavigate && navigation.to) {
        // Navigate to the intended destination

        cleanup?.()
        goto(navigation.to.url.pathname + navigation.to.url.search + navigation.to.url.hash).then(
          () => {
            navigationResolve = null
            suppressNavigationGuard = false
          }
        )
      } else {
        goto('').then(() => {
          navigationResolve = null
          suppressNavigationGuard = false
        })
      }
    })
  })

  // Warn before closing browser tab/window with unsaved changes
  const handleBeforeUnload = (e: BeforeUnloadEvent) => {
    if (!unsavedChanges) {
      return
    }
    // Avoid showing the custom "unsaved changes" dialog as the native browser one is displayed
    navigationResolve?.(false)
    navigationResolve = null
    suppressNavigationGuard = true
  }
</script>

<svelte:window onbeforeunload={handleBeforeUnload} />

{#snippet unsavedChangesDialog()}
  <DeleteDialog
    {...deleteDialogProps(
      'Discard changes',
      'Unsaved changes',
      () => {
        navigationResolve?.(true)
      },
      'You have unsaved changes in the pipeline code. Are you sure you want to leave without saving?'
    )()}
    onClose={() => {
      globalDialog.dialog = null
      navigationResolve?.(false)
    }}
  ></DeleteDialog>
{/snippet}
