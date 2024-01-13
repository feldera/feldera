'use client'

import { ProgramEditor } from '$lib/components/layouts/analytics/editor'
import { useSearchParams } from 'next/navigation'

const Editor = () => {
  // Get the project id from the URL
  const programName = useSearchParams().get('program_name') ?? ''

  return <ProgramEditor programName={programName} />
}

export default Editor
