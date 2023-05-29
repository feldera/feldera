import { useRouter } from 'next/router'
import { useEffect, useState } from 'react'
import { ReactFlowProvider } from 'reactflow'
import { PipelineId } from 'src/types/manager'
import { PipelineWithProvider } from '.'

const PipelineWithId = () => {
  const [pipelineId, setPipelineId] = useState<PipelineId | undefined>(undefined)
  const router = useRouter()
  const { id } = router.query

  useEffect(() => {
    if (typeof id === 'string') {
      setPipelineId(id)
    }
  }, [pipelineId, setPipelineId, id])

  return (
    pipelineId !== null && (
      <ReactFlowProvider>
        <PipelineWithProvider pipelineId={pipelineId} setPipelineId={setPipelineId} />
      </ReactFlowProvider>
    )
  )
}

export default PipelineWithId
