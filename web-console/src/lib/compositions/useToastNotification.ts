import { getContext } from 'svelte'
import { type ToastContext } from '@skeletonlabs/skeleton-svelte'

export const useToast = () => {
  const toast: ToastContext = getContext('toast')
  return {
    toastError(e: Error) {
      toast.create({
        title: (e.cause as any)?.name,
        description: e.message,
        type: 'error',
        duration: 15000
      })
    }
  }
}
