import { tuple } from '$lib/functions/common/tuple'
import { useRouter } from 'next/router'
import { useEffect, useState } from 'react'

// https://stackoverflow.com/questions/69343932/how-to-detect-change-in-the-url-hash-in-next-js
export const useHashPart = () => {
  const router = useRouter()
  const [hash, setHash] = useState(('window' in globalThis && window.location.hash) || '')

  const updateHash = (str: string) => {
    window.location.hash = str
    setHash(str)
  }

  useEffect(() => {
    const onWindowHashChange = () => {
      updateHash(window.location.hash.slice(1))
    }
    const onNextJSHashChange = (url: string) => {
      updateHash(url.split('#')[1] ?? '')
    }

    router.events.on('hashChangeStart', onNextJSHashChange)
    window.addEventListener('hashchange', onWindowHashChange)
    window.addEventListener('load', onWindowHashChange)
    return () => {
      router.events.off('hashChangeStart', onNextJSHashChange)
      window.removeEventListener('hashchange', onWindowHashChange)
      window.removeEventListener('load', onWindowHashChange)
    }
  }, [router.asPath, router.events])

  return tuple(hash, updateHash)
}
