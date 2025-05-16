import { toast } from 'svelte-french-toast'

export const useToast = () => {
  return {
    toastError(e: Error) {
      toast.error(e.message, {
        className: 'text-lg !max-w-[500px]'
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
