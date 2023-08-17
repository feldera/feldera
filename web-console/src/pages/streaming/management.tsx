import { ErrorOverlay } from '$lib/components/common/table/ErrorOverlay'
import PipelineTable from '$lib/components/streaming/management/PipelineTable'
import { ErrorBoundary } from 'react-error-boundary'

import Grid from '@mui/material/Grid'

const PipelineManagement = () => {
  return (
    <Grid container spacing={6} className='match-height'>
      {/* id referenced by webui-tester */}
      <Grid item xs={12} id='pipeline-management-content'>
        <ErrorBoundary FallbackComponent={ErrorOverlay}>
          <PipelineTable />
        </ErrorBoundary>
      </Grid>
    </Grid>
  )
}

export default PipelineManagement
