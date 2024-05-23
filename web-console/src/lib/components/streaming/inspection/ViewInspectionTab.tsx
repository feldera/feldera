import { ErrorOverlay } from '$lib/components/common/table/ErrorOverlay'
import { InspectionTable } from '$lib/components/streaming/inspection/InspectionTable'
import { Pipeline, PipelineStatus } from '$lib/types/pipeline'
import { ErrorBoundary } from 'react-error-boundary'

export const ViewInspectionTab = ({
  pipeline,
  caseIndependentName
}: {
  pipeline: Pipeline
  caseIndependentName: string
}) => {
  const logError = (error: Error) => {
    console.error('InspectionTable error: ', error)
  }

  return pipeline.state.current_status === PipelineStatus.RUNNING ||
    pipeline.state.current_status === PipelineStatus.PAUSED ? (
    <ErrorBoundary FallbackComponent={ErrorOverlay} onError={logError} key={location.pathname}>
      <InspectionTable pipeline={pipeline} caseIndependentName={caseIndependentName} />
    </ErrorBoundary>
  ) : (
    <ErrorOverlay error={new Error(`'${pipeline.descriptor.name}' is not deployed.`)} />
  )
}
