import Editors from '$lib/components/layouts/analytics/editor'
import { useRouter } from 'next/router'
import { useEffect, useState } from 'react'

const Editor = () => {
  // Get the project id from the URL
  const router = useRouter()
  const [programId, setProgramId] = useState<string | undefined | null>(undefined)
  useEffect(() => {
    const { program_id } = router.query
    if (router.isReady && typeof program_id === 'string') {
      setProgramId(program_id)
    } else {
      setProgramId(null)
    }
  }, [router, programId, setProgramId])

  return programId !== undefined && <Editors programId={programId} />
}

export default Editor
