import { toast } from 'svelte-french-toast'

export const useToast = () => {
  const showToastError = (scope: string, error: Error, durationMs?: number) => {
    try {
      const message = scope ? `${scope}: ${error.message}` : error.message
      toast.error(message, {
        className: 'text-lg !max-w-[500px] whitespace-pre-wrap',
        duration: durationMs
      })
    } catch (e) {
      console.log('Unable to push toast error:', e)
      console.log('Original error: ', error)
    }
  }
  const toastError = (scope: string) => (error: Error, durationMs?: number) => {
    showToastError(scope, error, durationMs)
  }
  return {
    toastError,
    catchError<Args extends any[], R>(
      scope: string,
      f: (...args: Args) => R,
      durationMs?: number
    ) {
      return (...args: Args) => {
        try {
          return f(...args)
        } catch (e) {
          if (e instanceof Error) {
            requestAnimationFrame(() => {
              showToastError(scope, e, durationMs)
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
