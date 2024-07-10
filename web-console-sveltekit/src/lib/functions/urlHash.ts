import { SetStateAction } from 'react'

export const showOnHashPart =
  ([hash, setHash]: [string, (str: string) => void]) =>
  (target: string | RegExp) => ({
    show: typeof target === 'string' ? hash.startsWith(target) : target.test(hash),
    setShow: (show: SetStateAction<boolean>) =>
      ((show) => setHash(show ? window.location.hash.slice(1) : ''))(
        show instanceof Function ? show(true) : show
      )
  })
