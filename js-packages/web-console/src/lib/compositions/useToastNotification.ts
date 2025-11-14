import { toast } from 'svelte-french-toast'

export const useToast = () => {
  return {
    toastError(e: Error, durationMs?: number) {
      toast.error(e.message, {
        className: 'text-lg !max-w-[500px]',
        duration: durationMs
      })
    },
    toastMain(message: string) {
      toast.error(message, {
        id: 'main',
        duration: 100000000000,
        position: 'top-center',
        className: 'text-lg !max-w-[500px]'
      })
    },
    dismissMain() {
      toast.dismiss('main')
    }
  }
}
