type Args = { clicked: string; base?: string; timeout?: number }

export function clickedClass(
  element: HTMLButtonElement,
  { clicked, base = '', timeout = 1000 }: Args
) {
  let baseClasses = base?.split(' ') ?? []
  let successClasses = clicked?.split(' ') ?? []
  let timeoutMs = timeout
  let _timeout: Timer | undefined
  element.classList.add(...baseClasses)
  const handleClick = () => {
    element.classList.remove(...baseClasses)
    element.classList.add(...successClasses)
    _timeout = setTimeout(() => {
      _timeout = undefined
      element.classList.remove(...successClasses)
      element.classList.add(...baseClasses)
    }, timeoutMs)
  }
  element.addEventListener('click', handleClick)
  return {
    update({ clicked, base = '', timeout = 1000 }: Args) {
      timeoutMs = timeout
      element.classList.remove(...(_timeout ? successClasses : baseClasses))
      baseClasses = base?.split(' ') ?? []
      successClasses = clicked?.split(' ') ?? []
      element.classList.add(...(_timeout ? successClasses : baseClasses))
    },
    destroy() {
      clearTimeout(_timeout)
      element.removeEventListener('click', handleClick)
    }
  }
}
