import { toast } from 'svelte-french-toast'

export const useToast = () => {
  const toastError = (error: Error, durationMs?: number) => {
    try {
      toast.error(error.message, {
        className: 'text-lg !max-w-[500px] whitespace-pre-wrap',
        duration: durationMs
      })
    } catch (e) {
      console.log('Unable to push toast error:', e)
      console.log('Original error: ', error)
    }
  }
  return {
    toastError,
    catchError<Args extends any[], R>(f: (...args: Args) => R, durationMs?: number) {
      return (...args: Args) => {
        try {
          return f(...args)
        } catch (e) {
          if (e instanceof Error) {
            requestAnimationFrame(() => {
              toastError(e, durationMs)
            })
          }
          return undefined
        }
      }
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
