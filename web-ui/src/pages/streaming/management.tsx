import Grid from '@mui/material/Grid'
import Typography from '@mui/material/Typography'
import { ErrorBoundary } from 'react-error-boundary'
import { ErrorOverlay } from 'src/components/table/ErrorOverlay'
import PageHeader from 'src/layouts/components/page-header'
import PipelineTable from 'src/streaming/management/PipelineTable'

const PipelineManagement = () => {
  return (
    <Grid container spacing={6} className='match-height'>
      <PageHeader
        title={<Typography variant='h5'>Pipeline Management</Typography>}
        subtitle={<Typography variant='body2'>Start, stop and inspect pipelines.</Typography>}
      />

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
