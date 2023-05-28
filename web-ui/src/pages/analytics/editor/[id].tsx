import { useState, useEffect } from 'react'
import { useRouter } from 'next/router'

import Editors from 'src/analytics/editor'

const Editor = () => {
  // Get the project id from the URL
  const router = useRouter()
  const [projectId, setProjectId] = useState<string | null>(null)
  useEffect(() => {
    const { id } = router.query
    if (typeof id === 'string') {
      setProjectId(id)
      console.log('setProjectId', id)
    }
  }, [router, projectId, setProjectId])

  return projectId != null ? (
    <Editors
      program={{
        program_id: projectId,
        name: '',
        description: '',
        status: 'None',
        version: 0,
        code: ''
      }}
    />
  ) : (
    '<p>Loading</p>'
  )
}

export default Editor
