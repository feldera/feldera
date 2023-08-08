import Grid from '@mui/material/Grid'
import Typography from '@mui/material/Typography'
import { ErrorBoundary } from 'react-error-boundary'
import { ErrorOverlay } from 'src/components/table/ErrorOverlay'
import { usePageHeader } from 'src/compositions/ui/pageTitle'
import PageHeader from 'src/layouts/components/page-header'
import PipelineTable from 'src/streaming/management/PipelineTable'

const PipelineManagement = () => {
  usePageHeader(s => s.setHeader)(
    <PageHeader title='Pipeline Management' subtitle='Start, stop and inspect pipelines.' />
  )
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
