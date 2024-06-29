import { tuple } from '$lib/functions/common/tuple'
import { useParams } from 'next/navigation'
import { useCallback, useEffect, useState } from 'react'

// https://stackoverflow.com/questions/69343932/how-to-detect-change-in-the-url-hash-in-next-js
export const useHashPart = <T extends string>() => {
  const [hash, setHash] = useState(('window' in globalThis && window.location.hash.slice(1)) || '')

  // https://github.com/vercel/next.js/discussions/49465#discussioncomment-5845312
  const params = useParams()
  useEffect(() => {
    const newHash = window.location.hash.split('#')[1] ?? ''
    if (hash !== newHash) {
      setHash(newHash)
    }
  }, [params, hash, setHash])

  const updateHash = useCallback(
    (str: T) => {
      window.location.hash = str
      setHash(str)
    },
    [setHash]
  )

  useEffect(() => {
    const onWindowHashChange = () => {
      updateHash(window.location.hash.slice(1) as T)
    }

    window.addEventListener('hashchange', onWindowHashChange)
    window.addEventListener('load', onWindowHashChange)
    return () => {
      window.removeEventListener('hashchange', onWindowHashChange)
      window.removeEventListener('load', onWindowHashChange)
    }
  }, [updateHash])

  return tuple(hash as T, updateHash)
}
