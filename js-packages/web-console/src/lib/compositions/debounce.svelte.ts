export const useDebounce = () => {
  let timeout = $state<ReturnType<typeof setTimeout>>()
  return <Args extends any[]>(callback: (...args: Args) => void, wait = 300) => {
    return (...args: Args) => {
      clearTimeout(timeout)
      timeout = setTimeout(() => callback(...args), wait)
    }
  }
}
