import { useRouter } from 'next/router'
import { useEffect, useState } from 'react'
import { ReactFlowProvider } from 'reactflow'
import { ConfigId } from 'src/types/manager'
import { PipelineWithProvider } from '.'

const PipelineWithId = () => {
  const [configId, setConfigId] = useState<ConfigId | undefined>(undefined)
  const router = useRouter()
  const { id } = router.query

  useEffect(() => {
    if (typeof id === 'string' && parseInt(id) != configId) {
      setConfigId(parseInt(id))
    }
  }, [configId, setConfigId, id])

  return (
    configId !== null && (
      <ReactFlowProvider>
        <PipelineWithProvider configId={configId} setConfigId={setConfigId} />
      </ReactFlowProvider>
    )
  )
}

export default PipelineWithId
