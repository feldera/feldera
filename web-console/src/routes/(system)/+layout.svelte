<script lang="ts">
  import { useToast } from '$lib/compositions/useToastNotification'
  const { toastError } = useToast()
  window.addEventListener('error', (event) => {
    if (event.message === 'ResizeObserver loop completed with undelivered notifications.') {
      // A fix for monaco-editor
      return
    }
    toastError(new Error(event.message, { cause: { name: 'Window error' } }))
  })
  window.addEventListener('unhandledrejection', (event) => {
    toastError(new Error(event.reason, { cause: { name: 'Unhandled rejection' } }))
  })
  window.addEventListener('rejectionhandled', (event) => {
    toastError(new Error(event.reason, { cause: { name: 'Rejection handled' } }))
  })
  let { children } = $props()
</script>

<svelte:boundary
  onerror={(error) => {
    if (error instanceof Error) {
      toastError(error)
    }
  }}
>
  {@render children()}
</svelte:boundary>
