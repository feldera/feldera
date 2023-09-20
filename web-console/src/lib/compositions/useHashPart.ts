import { tuple } from '$lib/functions/common/tuple'
import { useParams } from 'next/navigation'
import { useEffect, useState } from 'react'

// https://stackoverflow.com/questions/69343932/how-to-detect-change-in-the-url-hash-in-next-js
export const useHashPart = () => {
  const [hash, setHash] = useState(('window' in globalThis && window.location.hash) || '')

  // https://github.com/vercel/next.js/discussions/49465#discussioncomment-5845312
  const params = useParams()
  useEffect(() => {
    const newHash = window.location.hash.split('#')[1] ?? ''
    if (hash !== newHash) {
      setHash(newHash)
    }
  }, [params, hash, setHash])

  const updateHash = (str: string) => {
    window.location.hash = str
    setHash(str)
  }

  useEffect(() => {
    const onWindowHashChange = () => {
      updateHash(window.location.hash.slice(1))
    }

    window.addEventListener('hashchange', onWindowHashChange)
    window.addEventListener('load', onWindowHashChange)
    return () => {
      window.removeEventListener('hashchange', onWindowHashChange)
      window.removeEventListener('load', onWindowHashChange)
    }
  }, [])

  return tuple(hash, updateHash)
}
