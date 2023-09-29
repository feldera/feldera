'use client'

import Editors from '$lib/components/layouts/analytics/editor'
import { useSearchParams } from 'next/navigation'
import { useEffect, useState } from 'react'

const Editor = () => {
  // Get the project id from the URL
  const [programId, setProgramId] = useState<string | undefined | null>(undefined)
  const newProgramId = useSearchParams().get('program_id')
  useEffect(() => {
    setProgramId(newProgramId)
  }, [newProgramId, setProgramId])

  return programId !== undefined && <Editors programId={programId} />
}

export default Editor
